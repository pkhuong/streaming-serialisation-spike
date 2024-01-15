#include "producer.h"

#include <assert.h>
#include <cstring>

#define UNLIKELY(X) __builtin_expect(!!(X), 0)
#define LIKELY(X) __builtin_expect(!!(X), 1)

SubstreamFrame::~SubstreamFrame() = default;

SubstreamFrame::Buffer::Buffer()
{
    // XXX These are a bit rounder than necessary.
    metadata.resize(max_metadata_size + 8);  // Plus overflow bytes
    data.resize(128 + 128 + 64);  // Up to 128 bytes of buffered data, plus 128 more for the new field, plus overflow bytes
}

bool SubstreamFrame::varlen_field(FieldIndex field, bool force_last /* = false */)
{
    assert(!closed_);
    assert(!is_in_varlen_field_);

    // Can't add a field once we had a large implicit one.
    if (UNLIKELY(!buffer_.has_value()))
        return false;  // Unreachable: `!is_in_varlen_field_ && !closed_`.

    Buffer &buffer = buffer_.value();

    // Bail if the metadata is finalized (already terminated).
    if (UNLIKELY(buffer.metadata_was_finalized))
        return false;

    // If we're not finalized, there must be room for at least the sentinel byte.
    assert(buffer.metadata_written < max_metadata_size);

    size_t num_out_of_line_bytes = field.num_radix_128_digits();

    // Make sure we have room.
    {
        size_t minimum_metadata_size = 1 + num_out_of_line_bytes;
        size_t metadata_space_remaining = max_metadata_size - buffer.metadata_written;

        if (UNLIKELY(minimum_metadata_size > metadata_space_remaining))  // can't add metadata, decline.
            return false;

        // Can't have a size suffix byte if there's no room for field
        // plus metadata (minimum size) *and* size suffix and terminator.
        force_last |= (minimum_metadata_size + 2 > metadata_space_remaining);
    }

    // Cache the write index locally, in a large register.
    size_t metadata_write_index = buffer.metadata_written;
    memcpy(&buffer.metadata[metadata_write_index], field.get_out_of_line_bytes(),
           FieldIndex::out_of_line_bytes_avail);

    metadata_write_index += num_out_of_line_bytes;

    // XXX: encode more nicely.
    uint8_t combined_byte = 128  + uint8_t(field.get_type()) + (force_last ? 0 : 21);
    buffer.metadata[metadata_write_index++] = std::byte(combined_byte);

    assert(metadata_write_index <= max_metadata_size);
    buffer.metadata_written = metadata_write_index;

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

    // in verbatim mode, we hand out large buffers, so calls should be rare
    if (UNLIKELY(is_verbatim()))
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
    if (UNLIKELY(buffer.data.size() - buffer.data_committed < span_size + 64))
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

    if (UNLIKELY(is_verbatim()))  // With large spans, commit calls should be rare.
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
    if (LIKELY(written <= max_inline_data_size - buffer.current_field_size))
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

    if (UNLIKELY(is_verbatim()))
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
    if (LIKELY(!is_verbatim()))
    {
        Buffer &buffer = buffer_.value();

        assert(buffer.metadata_written <= max_metadata_size);

	// metadata_write_index and buffer.metadata_written are kept in sync.
	//
	// Locally, prefer to use `metadata_write_index`, since it
	// clearly doesn't overflow.
	size_t metadata_write_index = buffer.metadata_written;

        // Add a sentinel if the last metadata byte isn't an implicit length terminator
	assert(buffer.metadata_written == metadata_write_index);
#if 0
        if (!buffer.metadata_was_finalized)
        {
            assert(buffer.metadata_written < max_metadata_size);
            buffer.metadata[buffer.metadata_written++] = std::byte(-108);  // XXX: constant
            assert(buffer.metadata_written <= max_metadata_size);

	    metadata_write_index = buffer.metadata_written;
        }
#else
	assert(buffer.metadata_was_finalized || metadata_written < max_metadata_size);
	buffer.metadata[metadata_write_index] = std::byte(-108);  // XXX: constant

	metadata_write_index += buffer.metadata_was_finalized ? 0 : 1;

	buffer.metadata_written = metadata_write_index;

	assert(buffer.metadata_written <= max_metadata_size);
#endif
	assert(buffer.metadata_written == metadata_write_index);

        // There must be metadata
        assert(buffer.metadata_written > 0);
        // And the last byte must be for an implicit length.
        assert(int8_t(buffer.metadata[buffer.metadata_written - 1]) <= -108);

        size_t total_size = buffer.data_committed + metadata_write_index;

        // We can just write this in the parent, as an inline payload.
        if (LIKELY(nesting_level_ > 0 && total_size <= max_inline_data_size - 1))
        {
            SubstreamFrame *parent = &context.frames[nesting_level_ - 1];

	    std::span<std::byte> dst = parent->get_buffer(context);

#ifdef SUBSTREAM_TEST_CONSTANTS
	    // When test constants are enabled, the magic numbers below
	    // don't really work well.
	    const bool simple_mode = true;
#else
	    const bool simple_mode = false;
#endif
	    if (LIKELY(dst.size() >= 128 && !simple_mode && total_size < 128))
	    {
		dst[0] = std::byte(total_size);
		// We know we can write up to 64 bytes past the end, and
		// the metadata buffer is preallocated with more than 128
		// bytes.
		assert(buffer.metadata.size() >= 128);
		// XXX: should we go for shorter copies?
		memcpy(&dst[1], buffer.metadata.data(), 128);

		size_t dst_index = 1 + metadata_write_index;
		assert(dst_index <= 128);

		memcpy(&dst[dst_index], buffer.data.data(), 64);
		dst_index += 64;
		if (LIKELY(buffer.data_committed > 64)) // bias codegen toward doing more work
		{
		    memcpy(&dst[dst_index], buffer.data.data() + 64, 64);
		    dst_index += 64;
		}

		// We must have copied at least the header + total size.
		assert(dst_index >= 1 + total_size);

		parent->commit(context, 1 + total_size);
	    }
	    else
	    {
		// We need the chunk size header.
		const std::byte header[1] = { std::byte(total_size) };

		parent->write(context, header);
		parent->write(context, std::span(buffer.metadata.data(), metadata_write_index));
		parent->write(context, std::span(buffer.data.data(), buffer.data_committed));
	    }

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
