#include "framing.h"

// Out of line to only have one vtble.
BufferProvider::~BufferProvider() = default;

void BaseFrameWriter::close(FrameWriterState state)
{
    assert(check_rep());

    if (chunk_header_[0].is_valid())
        finalize_current_chunk(state.provider);

    // The calling code could have already trigger finalisation.
    // We should be in a valid state here.
    assert(check_rep());

    // The last chunk we dumped was full.  Dump an empty one.
    if (last_chunk_full_)
    {
        // These two calls should leave us in a valid state.
        get_buffer(state);

        assert(check_rep());

        finalize_current_chunk(state.provider);
        assert(check_rep());

        // And the last chunk shouldn't be full anymore: we just
        // closed an empty chunk.
        assert(!last_chunk_full_);
    }

    commit_to_parent(state);

    assert(check_rep());

    // Clear our state.
    *this = BaseFrameWriter(nesting_level_);

    assert(check_rep());
}

bool BaseFrameWriter::check_rep() const
{
    assert(check_buf_rep());

    // We may only have data bytes in the current chunk if we have a chunk header.
    assert(chunk_header_[0].is_valid() || data_bytes_committed_ == 0);

    // If the first chunk header byte is invalid, so's the second.
    assert(chunk_header_[0].is_valid() || !chunk_header_[1].is_valid());

    // If we have a header and we're past the initial frame, the header spans two bytes.
    assert(!chunk_header_[0].is_valid() || chunk_header_[initial_chunk_ ? 0 : 1].is_valid());

    // We never have a full chunk.
    assert(data_bytes_committed_ < max_chunk_size());

    // We can't have a full chunk if we're still in the initiial frame.
    assert(!(initial_chunk_ && last_chunk_full_));

    return true;
}

bool BaseFrameWriter::check_buf_rep() const
{
    // The buffer is valid iff the initial offset is valid.
    assert((buffer_.is_valid()) == (buffer_initial_offset_ != uint64_t(-1)));

    // We can't have a non-empty invalid buffer.
    assert(buffer_.is_valid() || buffer_.write_buf.empty());

    return true;
}

void BaseFrameWriter::ensure_non_empty_buffer_impl(FrameWriterState state)
{
    assert(check_buf_rep());

    // We must only be called when the buffer is empty (or invalid)
    assert(buffer_.write_buf.empty());

    // This variable is only read when buffer_ is valid.
    size_t written_to_buffer = buffer_.first_offset - buffer_initial_offset_;

    if (nesting_level_ == 0)
    {
        // We're the toplevel frame, grab directly from the buffer provider.
        BufferProvider *provider = state.provider;

        if (!buffer_.is_valid())
        {
            // Initial buffer.
            buffer_ = provider->get_buffer();
            assert(!buffer_.write_buf.empty());
        }
        else
        {
            buffer_ = provider->refresh_buffer(written_to_buffer);
            assert(!buffer_.write_buf.empty());
        }
    }
    else
    {
        BaseFrameWriter *parent = &state.writers[nesting_level_ - 1];

        if (buffer_.is_valid())
            parent->commit(state, written_to_buffer);

        buffer_ = parent->get_buffer(state);
        assert(!buffer_.write_buf.empty());
    }

    buffer_initial_offset_ = buffer_.first_offset;

    assert(buffer_.is_valid());
    assert(!buffer_.write_buf.empty());

    assert(check_buf_rep());
}

void BaseFrameWriter::commit_to_parent(FrameWriterState state)
{
    assert(check_buf_rep());

    size_t written_to_buffer = buffer_.first_offset - buffer_initial_offset_;

    if (buffer_.is_valid() && written_to_buffer > 0)
    {
        if (nesting_level_ == 0)
        {
            state.provider->commit(written_to_buffer);
        }
        else
        {
            state.writers[nesting_level_ - 1].commit(state, written_to_buffer);
        }
    }

    buffer_ = Buffer();
    buffer_initial_offset_ = -1;

    assert(!buffer_.is_valid());

    assert(check_buf_rep());
}

Buffer BaseFrameWriter::get_buffer_slow(FrameWriterState state)
{
    assert(check_rep());

    ensure_non_empty_buffer(state);

    // Do we have to prepare a new chunk?
    if (!chunk_header_[0].is_valid())
    {
        // If so, reserve the header byte(s).
        size_t backref_count = initial_chunk_ ? 1 : 2;

        for (size_t i = 0; i < backref_count; i++)
        {
            assert(!buffer_.write_buf.empty());
            chunk_header_[i] = buffer_.register_backref();
            ensure_non_empty_buffer(state);
        }

        assert(chunk_header_[0].is_valid());
    }

    assert(!buffer_.write_buf.empty());
    assert(check_rep());

    Buffer ret = buffer_.first(max_chunk_size() - data_bytes_committed_);
    assert(!ret.write_buf.empty());
    return ret;
}

void BaseFrameWriter::finalize_current_chunk(BufferProvider *provider)
{
    if (initial_chunk_)
    {
        assert(data_bytes_committed_ <= max_initial_chunk_size);

        // We should only have one chunk header byte.
        assert(chunk_header_[0].is_valid());
        assert(!chunk_header_[1].is_valid());

        const std::pair<Backref, uint8_t> to_patch[] = {
            {chunk_header_[0], data_bytes_committed_}
        };
        provider->fill_backrefs(to_patch);
        last_chunk_full_ = data_bytes_committed_ == max_initial_chunk_size;
    }
    else
    {
        assert(data_bytes_committed_ <= max_followup_chunk_size);

        // We should have two chunk header bytes.
        assert(chunk_header_[0].is_valid());
        assert(chunk_header_[1].is_valid());

        uint8_t lo = data_bytes_committed_ % 253;
        uint8_t hi = data_bytes_committed_ / 253;

        const std::pair<Backref, uint8_t> to_patch[] = {
            {chunk_header_[0], lo},
            {chunk_header_[1], hi},
        };

        provider->fill_backrefs(to_patch);
        last_chunk_full_ = data_bytes_committed_ == max_followup_chunk_size;
    }

    initial_chunk_ = false;
    data_bytes_committed_ = 0;
    chunk_header_[0] = Backref();
    chunk_header_[1] = Backref();

    assert(!chunk_header_[0].is_valid());
    assert(check_rep());
}

NestedFrameWriter::~NestedFrameWriter() = default;

void NestedFrameWriter::pop_all_frames()
{
    while (!writers_.empty())
        pop_frame();
}

void NestedFrameWriter::write(std::span<const std::byte> frame_data, bool force_write_if_empty /* = false */)
{
    assert(!writers_.empty());

    while (!frame_data.empty() || force_write_if_empty)
    {
        std::span<std::byte> dst = get_buffer();
        assert(!dst.empty());

        if (frame_data.empty())
            break;

        // Round up the destination size to 64 bytes, since we can always write a little
        // past the end of `dst`.
        size_t writable = (dst.size() + 63) & -64UL;
        memcpy(dst.data(), frame_data.data(), std::min(writable, frame_data.size()));

        // But only commit what was actually available.
        size_t written = std::min(dst.size(), frame_data.size());
        commit(written);

        frame_data = frame_data.subspan(written);
        force_write_if_empty = false;
    }

    last_buffer_size_ = -1;
}
