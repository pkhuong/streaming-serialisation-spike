#include "producer.h"

#include <assert.h>
#include <cstring>

SubstreamFrame::~SubstreamFrame() = default;

SubstreamFrame::Buffer::Buffer()
{
    // XXX These are a bit rounder than necessary.
    metadata.resize(max_metadata_size + 8);  // Plus overflow bytes
    data.resize(128 + 128 + 64);  // Up to 128 bytes of buffered data, plus 128 more for the new field, plus overflow bytes
}

bool SubstreamFrame::varlen_field(uint64_t field, bool force_last /* = false */)
{
    // validate inputs
    assert(field > 0);
    // XXX: take pre-encoded field.
    assert(field < 16);
    assert(!closed_);
    assert(!is_in_varlen_field_);

    // Can't add a field once we had a large implicit one.
    if (!buffer_.has_value())
        return false;  // Unreachable: `!is_in_varlen_field_ && !closed_`.

    Buffer &buffer = buffer_.value();

    // Bail if the metadata is finalized (already terminated).
    if (buffer.metadata_was_finalized)
        return false;

    // If we're not finalized, there must be room for at least the sentinel byte.
    assert(buffer.metadata_written < max_metadata_size);

    {
        size_t minimum_metadata_size = 1;  // XXX should count out of line field bytes.
        size_t metadata_space_remaining = max_metadata_size - buffer.metadata_written;
        // We're gonna need at least one (XXX field size) byte for the metadata.
        if (minimum_metadata_size > metadata_space_remaining)  // can't add metadata, decline.
            return false;

        // Can't have a size suffix byte if there's no room for field
        // plus metadata (minimum size) *and* size suffix and terminator.
        force_last |= (minimum_metadata_size + 2 > metadata_space_remaining);
    }

    // XXX: encode more nicely.
    uint8_t combined_byte = 128 + (field - 1) + (force_last ? 0 : 21);

    buffer.metadata[buffer.metadata_written++] = std::byte(combined_byte);

    // New field, new size counter.
    buffer.current_field_size = 0;
    buffer.metadata_was_finalized = force_last;

    is_in_varlen_field_ = true;
    return true;
}

std::span<std::byte> SubstreamFrame::get_buffer(ProducerContext context)
{
    assert(!closed_);
    assert(is_in_varlen_field_);  // XXX: validation

    if (is_verbatim())
    {
        // If the `get_buffer()` call gets to us and we're in verbatim
        // mode, we must be the toplevel.
        NestedFrameWriter *writer = context.writer;
        assert(writer->get_depth() == nesting_level_ + 1);

        std::span<std::byte> ret = writer->get_buffer();
        assert(!ret.empty());
        return ret;
    }

    assert(buffer_.has_value());
    Buffer &buffer = buffer_.value();

    // Otherwise we'd be in verbatim mode.
    assert(buffer.current_field_size <= max_inline_data_size);

    // We must have metadata
    assert(buffer.metadata_written > 0);
    // The metadata must be for a varlen field.
    assert((uint8_t(buffer.metadata[buffer.metadata_written - 1]) - 128) / 21 <= 1);

    // Give the caller enough rope to go one past `max_inline_data_size`
    size_t span_size = max_inline_data_size + 1;

    assert(buffer.data_committed <= buffer.data.size());
    // SLOW PATH: must switch to large buffering state.
    if (buffer.data.size() - buffer.data_committed < span_size + 64)
    {
        // XXX: round this?
        size_t wanted_data_capacity = max_data_size + 128 + 64;
        assert(buffer.data.size() < wanted_data_capacity);

        // If we get here, we were in a "small" buffer state and now need to be "large."
        assert(buffer.metadata_written > 0);
        assert(buffer.data_committed >= 127);

        // Tell the parent we're gonna be > 127 bytes for sure.
        //
        // We don't upgrade the field (*this* substream) to implicit
        // length because it might still fit in 127 bytes.
        if (nesting_level_ > 0)
            context.frames[nesting_level_ - 1].switch_to_verbatim(context);

        // XXX: should we grow adaptively? We can use chunks (only
        // need to hand out 128-byte spans), maybe do that instead.
        //
        // XXX: can use round 256-byte copy.
        buffer.data.resize(wanted_data_capacity);
    }

    assert(buffer.data.size() - buffer.data_committed >= span_size + 64);
    std::span<std::byte> ret(&buffer.data[buffer.data_committed], span_size);
    assert(!ret.empty());
    return ret;
}

void SubstreamFrame::commit(ProducerContext context, size_t written)
{
    assert(!closed_);
    assert(is_in_varlen_field_);  // XXX: validation

    if (is_verbatim())
    {
        // If the `commit()` call gets to us, we must be the toplevel.
        NestedFrameWriter *writer = context.writer;
        assert(writer->get_depth() == nesting_level_ + 1);

        writer->commit(written);
        return;
    }

    assert(buffer_.has_value());
    Buffer &buffer = buffer_.value();

    assert(is_in_varlen_field_);  // XXX: validation
    assert(written <= 128);  // XXX: validation, we don't hand out spans bigger than that.

    // We must have metadata
    assert(buffer.metadata_written > 0);
    // for a varlen field
    assert((uint8_t(buffer.metadata[buffer.metadata_written - 1]) - 128) / 21 <= 1);

    assert(buffer.data_committed <= buffer.data.size());
    buffer.data_committed += written;
    assert(buffer.data_committed <= buffer.data.size());

    // Otherwise we'd already be in verbatim mode.
    assert(buffer.current_field_size <= max_inline_data_size);
    if (written <= max_inline_data_size - buffer.current_field_size)
    {
        buffer.current_field_size += written;
        return;
    }

    // SLOW PATH HERE.  The current field exceeds 127 bytes,
    // so we must switch to implicit length... and clearly the
    // whole substream exceeds 127 bytes.
    switch_to_verbatim(context);
}

void SubstreamFrame::write(ProducerContext context, std::span<const std::byte> field_data)
{
    while (!field_data.empty())
    {
        std::span<std::byte> dst = get_buffer(context);
        assert(!dst.empty());

        // Round up the destination size to 64 bytes, since we can always write a little
        // past the end of `dst`.
        size_t writable = (dst.size() + 63) & -64UL;
        memcpy(dst.data(), field_data.data(), std::min(writable, field_data.size()));

        // But only commit what was actually available.
        size_t written = std::min(dst.size(), field_data.size());
        commit(context, written);

        field_data = field_data.subspan(written);
    }
}

void SubstreamFrame::close_varlen_field()
{
    assert(!closed_);
    assert(is_in_varlen_field_);  // XXX: validation

    if (is_verbatim())
    {
        // Nothing to do to denote the end of the final varlen field.
        is_in_varlen_field_ = false;
        return;
    }

    assert(buffer_.has_value());
    Buffer &buffer = buffer_.value();

    // Otherwise we'd already be in verbatim mode.
    assert(buffer.current_field_size <= max_inline_data_size);

    // We must have opened a field.
    assert(buffer.metadata_written > 0);
    if (buffer.metadata_was_finalized)
    {
        assert((uint8_t(buffer.metadata[buffer.metadata_written - 1]) - 128) / 21 == 0);
    }
    else
    {
        assert((uint8_t(buffer.metadata[buffer.metadata_written - 1]) - 128) / 21 == 1);
        // We always make sure there's room for the size byte, and a terminator.
        assert(buffer.metadata_written + 2 <= max_metadata_size);
    }

    // It's always safe to write 8 extra bytes; here; we're writing up to one extra byte.
    buffer.metadata[buffer.metadata_written] = std::byte(buffer.current_field_size);
    // Don't commit that byte if the metadata was finalized (with an implicit
    // length field).
    buffer.metadata_written += buffer.metadata_was_finalized ? 0 : 1;

    assert(buffer.metadata_written <= max_metadata_size);
    // Either we terminated metadata, or we have room for a sentinel.
    assert(buffer.metadata_was_finalized || buffer.metadata_written < max_metadata_size);

    // XXX: poison current_field_size.
    is_in_varlen_field_ = false;
}

void SubstreamFrame::close(ProducerContext context)
{
    assert(!closed_);

    // See if we have to switch to verbatim state.
    if (!is_verbatim())
    {
        Buffer &buffer = buffer_.value();

        assert(buffer.metadata_written <= max_metadata_size);

        // Add a sentinel if the last metadata byte isn't an implicit length terminator
        if (!buffer.metadata_was_finalized)
        {
            assert(buffer.metadata_written < max_metadata_size);
            buffer.metadata[buffer.metadata_written++] = std::byte(-108);  // XXX: constant
            assert(buffer.metadata_written <= max_metadata_size);
        }

        // There must be metadata
        assert(buffer.metadata_written > 0);
        // And the last byte must be for an implicit length.
        assert(int8_t(buffer.metadata[buffer.metadata_written - 1]) <= -108);

        size_t total_size = buffer.data_committed + buffer.metadata_written;

        // We can just write this in the parent, as an inline payload.
        if (nesting_level_ > 0 && total_size <= max_inline_data_size - 1)
        {
            SubstreamFrame *parent = &context.frames[nesting_level_ - 1];

            // We need the chunk size header.
            const std::byte header[1] = { std::byte(total_size) };
            parent->write(context, header);
            parent->write(context, std::span(buffer.metadata.data(), buffer.metadata_written));
            parent->write(context, std::span(buffer.data.data(), buffer.data_committed));

            buffer_.reset();
            closed_ = true;
            return;
        }

        // This will push a nested frame and dump all currently buffered data in.
        switch_to_verbatim_impl(context);
    }

    // We're (now) in verbatim mode.  Pop the nested frame.
    NestedFrameWriter *writer = context.writer;
    assert(writer->get_depth() == nesting_level_ + 1);
    writer->pop_frame();

    closed_ = true;
}

void SubstreamFrame::switch_to_verbatim_impl(ProducerContext context)
{
    assert(!closed_);
    assert(this == &context.frames[nesting_level_]);

    // Already verbatim, nothing to do.
    if (is_verbatim())
    {
        // All surrounding frames must also be verbatim
        assert(nesting_level_ == 0 || context.frames[nesting_level_ - 1].is_verbatim());
        return;
    }

    // First make all parent frames flush any buffered state.
    if (nesting_level_ > 0)
        context.frames[nesting_level_ - 1].switch_to_verbatim(context);

    // !verbatim, so we have a buffer.
    assert(buffer_.has_value());
    Buffer &buffer = buffer_.value();

    // Check the last metadata byte, make sure it's implicit length.
    {
        // We must have metadata.
        assert(buffer.metadata_written > 0);

        std::byte &terminator = buffer.metadata[buffer.metadata_written - 1];
        uint8_t metadata_value = uint8_t(terminator) - 128;

        // The last metadata must be set for a variable length field.
        assert(metadata_value / 21 <= 1);

        // Switch to implicit length if explicit.
        metadata_value -= (metadata_value >= 21) ? 21 : 0;

        // We now have a terminator field (implicit varlen).
        terminator = std::byte(128 + metadata_value);
        assert((uint8_t(terminator) - 128) / 21 == 0);
        assert(int8_t(terminator) <= -108);

        // We should set `metadata_was_finalized = true`, but the
        // whole buffer_ is about to be destroyed.
    }

    // Only substream frames in verbatim state have a writer frame, and we just
    // switched to verbatim, so our child (if any) isn't.
    NestedFrameWriter *writer = context.writer;
    assert(writer->get_depth() == nesting_level_);

    writer->push_frame();  // Get ourselves a frame

    // XXX: writes and reads could be sloppy for speed here.
    assert(buffer.metadata_written > 0);
    writer->write(std::span(buffer.metadata.data(), buffer.metadata_written));
    writer->write(std::span(buffer.data.data(), buffer.data_committed));

    // Switch to verbatim mode.
    buffer_.reset();
}

Producer::~Producer() = default;
