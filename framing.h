#pragma once

#include <assert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#ifdef NDEBUG
#define INLINE inline __attribute__((always_inline))
#else
#define INLINE inline __attribute__((noinline))
#endif

// A backref location is simply a 64 bit logical write offset.
struct Backref
{
    INLINE bool is_valid() const { return offset != uint64_t(-1); }

    uint64_t offset = -1;
};

// Buffers have the property that we can always write at least 64
// extra bytes past the nominal end of the buffer.
struct Buffer
{
    // Returns whether the buffer is valid.  A default-initialized
    // buffer isn't valid.
    INLINE bool is_valid() const { return backref_counter != nullptr && next_backref_location != nullptr; }

    // Consumes `count` bytes from `write_buf`.
    INLINE void advance(size_t count)
    {
        assert(count <= write_buf.size());
        write_buf = write_buf.subspan(count);
        first_offset += count;
    }

    // Registers a new backref location at the buffer's current
    // write location, and advances the write buf by 1.
    INLINE Backref register_backref()
    {
        assert(first_offset >= *next_backref_location);
        Backref ret(first_offset);
        advance(1);

        *backref_counter = *backref_counter + 1;
        *next_backref_location = first_offset;

        return ret;
    }

    // Returns a fresh buffer pointing to the same storage as this
    // buffer, with at most `prefix` bytes left in `write_buf`.
    INLINE Buffer first(size_t prefix) const
    {
        Buffer ret = *this;

        ret.write_buf = ret.write_buf.first(std::min(ret.write_buf.size(), prefix));
        return ret;
    }

    std::span<std::byte> write_buf;
    uint64_t first_offset = 0;  // The logical write ofset of the first byte in `write_buf`.

    // The pointee is incremented whenever a new backref is registered
    uint32_t *backref_counter = nullptr;
    // The pointee is the next minimal logical offset for a backref:
    // backref locations must be registered in strictly increasing
    // order.
    uint64_t *next_backref_location = nullptr;
};

class BufferProvider
{
public:
    BufferProvider() = default;
    virtual ~BufferProvider();

    // Not movable or copyable
    BufferProvider(const BufferProvider &) = delete;
    BufferProvider(BufferProvider &&) = delete;
    BufferProvider &operator=(const BufferProvider &) = delete;
    BufferProvider &operator=(BufferProvider &&) = delete;

    // Returns a non-empty buffer.  The return value may point back to
    // the `BufferProvider`, so the current object must outlive the
    // returned Buffer.
    //
    // The return value is non-empty and only valid until the next
    // call to `get_buffer()` of `commit()`.
    //
    // The return value is a `Buffer`, so it's always safe to write up
    // to 64 bytes past the end of the return value.
    virtual Buffer get_buffer() = 0;

    // Commits `count` bytes written to the head of the last buffer returned by `get_buffer()`.
    //
    // The `count` value must not exceed the size of the buffer last returned by `get_buffer()`.
    virtual void commit(size_t count) = 0;

    // A combination of `commit(count)` and `get_buffer()`.
    // Implementations of the `BufferProvider` interface may implement
    // this more efficiently than two virtual calls.
    //
    // The return value is a `Buffer`, so it's always safe to write up
    // to 64 bytes past the end of the return value.
    virtual Buffer refresh_buffer(size_t count)
    {
        commit(count);
        return get_buffer();
    }

    // Writes `data` to the buffer provider.  This can be implemented
    // more efficiently.
    virtual void write(std::span<const std::byte> src)
    {
        if (src.empty())
            return;

        std::span<std::byte> dst = get_buffer().write_buf;
        assert(dst.size() >= 1);

        while (!src.empty())
        {

            // Round up the destination size to 64 bytes, since we can always write a little
            // past the end of `dst`.
            size_t writable = (dst.size() + 63) & -64UL;
            memcpy(dst.data(), src.data(), std::min(writable, src.size()));

            // But only commit what was actually available.
            size_t written = std::min(dst.size(), src.size());

            dst = refresh_buffer(written).write_buf;
            assert(dst.size() >= 1);

            src = src.subspan(written);
        }
    }

    // Backrefes values (the second value of each pair) to the backref locations (the first
    // value of each pair).
    virtual void fill_backrefs(std::span<const std::pair<Backref, uint8_t>> target_values) = 0;
};

class BaseFrameWriter;

struct FrameWriterState
{
    BaseFrameWriter *writers = nullptr;
    BufferProvider *provider = nullptr;
};

class BaseFrameWriter
{
#ifndef FRAME_TEST_CONSTANTS
    static inline constexpr size_t max_initial_chunk_size = 253 - 1;
    static inline constexpr size_t max_followup_chunk_size = 253 * 253 - 1;
#else
    static inline constexpr size_t max_initial_chunk_size = 5;
    static inline constexpr size_t max_followup_chunk_size = 11;
#endif

public:
    explicit BaseFrameWriter(size_t nesting_level)
        : nesting_level_(nesting_level)
    {
        assert(check_rep());
    }

    ~BaseFrameWriter() { assert(check_rep()); }

    // Copyable and movable
    BaseFrameWriter(const BaseFrameWriter &) = default;
    BaseFrameWriter(BaseFrameWriter &&) = default;
    BaseFrameWriter &operator=(const BaseFrameWriter &) = default;
    BaseFrameWriter &operator=(BaseFrameWriter &&) = default;

    // Returns a non-empty destination buffer for the data to write in this frame.
    //
    // The return value is only valid until the next call to
    // `get_buffer()`, `commit()` or `close()`.
    //
    // It's always safe to write up to 64 bytes past the end of the return value.
    //
    // TODO: add variant that's the same except returns a std::span directly;
    // only matters for the out of line `get_buffer_slow()` call.
    INLINE Buffer get_buffer(FrameWriterState state)
    {
        assert(check_rep());

        size_t current_max_chunk_size = max_chunk_size();
        // If we have room and the header is setup, just return the un-committed
        // bytes until the end of the buffer_ or the end of the chunk (whichever
        // comes first).
        if (!buffer_.write_buf.empty() && chunk_header_[0].is_valid())
        {
            Buffer ret = buffer_.first(current_max_chunk_size - data_bytes_committed_);

            assert(!ret.write_buf.empty());
            assert(check_rep());
            return ret;
        }

        // Otherwise, we may have to refresh the buffer and/or setup
        // the chunk header bytes.
        //
        // The slow path checks for post conditions: return is
        // non-empty, and `check_rep()` is true.
        return get_buffer_slow(state);
    }

    // Commits `count` bytes written to the buffer last returned by `get_buffer()`.
    INLINE void commit(FrameWriterState state, size_t count)
    {
        assert(check_rep());

        // XXX: this is validation.
        assert(data_bytes_committed_ + count <= max_chunk_size());
        assert(count <= buffer_.write_buf.size());

        // Note the newly written bytes.
        advance_destination_buffer(count);

        // If we reached the end of the chunk, close it (backfill the
        // header byte(s) and prepare for the next chunk).
        if (data_bytes_committed_ == max_chunk_size())
            finalize_current_chunk(state.provider);

        assert(check_rep());
    }

    // Closes the current frame if any, and prepares to write to a
    // fresh frame (without actually opening the new frame).
    void close(FrameWriterState state);

private:
    // Check internal invariants.
    bool check_rep() const;

    // Check buffer management-specific invariants.
    bool check_buf_rep() const;

    INLINE size_t max_chunk_size() const
    {
        return initial_chunk_ ? max_initial_chunk_size : max_followup_chunk_size;
    }

    // Internal buffer management: these (and only these) methods
    // only manipulate buffer_ and buffer_initial_offset_.
    //
    // Other methods may advance buffer_, but not recycle/commit it.
    void ensure_non_empty_buffer_impl(FrameWriterState state);
    INLINE void ensure_non_empty_buffer(FrameWriterState state)
    {
        assert(check_buf_rep());

        if (buffer_.write_buf.empty())
            ensure_non_empty_buffer_impl(state);

        assert(check_buf_rep());

        assert(!buffer_.write_buf.empty());
    }

    // Notices the parent of any outstanding write in `buffer_`.
    void commit_to_parent(FrameWriterState state);

    // Chunk management: `advance_destination_buffer()` provides the
    // glue between chunk state (keeping track of the header backref
    // locations and counting the bytes already committed to the
    // current chunk), and the buffer state.

    // Returns a non-empty write buffer for the current (potentially
    // just opened) chunk.
    //
    // This slow path handles refreshing the current `buffer_`.
    Buffer get_buffer_slow(FrameWriterState state);

    INLINE void advance_destination_buffer(size_t count)
    {
        assert((buffer_.is_valid()) == (buffer_initial_offset_ != uint64_t(-1)));

        buffer_.advance(count);
        data_bytes_committed_ += count;

        assert((buffer_.is_valid()) == (buffer_initial_offset_ != uint64_t(-1)));
    }

    // Populate the current chunk's header and clears the chunk state
    // for the next chunk.
    void finalize_current_chunk(BufferProvider *provider);

    Buffer buffer_;
    uint64_t buffer_initial_offset_ = -1;

    uint32_t nesting_level_ = 0;  // Write-once, in constructor.

    uint32_t data_bytes_committed_ = 0; // Data bytes committed to current chunk
    std::array<Backref, 2> chunk_header_;

    bool initial_chunk_ = true;
    bool last_chunk_full_ = false;
};

class NestedFrameWriter
{
public:
    explicit NestedFrameWriter(BufferProvider *provider)
        : provider_(provider)
    {}

    ~NestedFrameWriter();

    // move-only: having two NestedFrameWriters target the same buffer
    // provider concurrently is a recipe for trouble.
    NestedFrameWriter(const NestedFrameWriter &) = delete;
    NestedFrameWriter(NestedFrameWriter &&) = default;
    NestedFrameWriter &operator=(const NestedFrameWriter &) = delete;
    NestedFrameWriter &operator=(NestedFrameWriter &&) = default;

    // Returns the current frame nesting depth.  0 means no frame open,
    // 1 means only a toplevel frame, etc.
    INLINE size_t get_depth() const { return writers_.size(); }

    // Returns the backing buffer provider if there is currently no open
    // frame; returns nullptr if we have frames in flight.
    INLINE BufferProvider *get_buffer_provider() const { return writers_.empty() ? provider_ : nullptr; }

    // Prepares for `depth` more nesting on top of what's currently open.
    INLINE void reserve_additional_depth(size_t depth)
    {
        // XXX: ensure geometric growth.
        writers_.reserve(writers_.size() + depth);
    }

    // Nests another frame.  Invalidates any pending write buffer.
    INLINE void push_frame()
    {
        writers_.emplace_back(writers_.size());
        last_buffer_size_ = -1;

        assert(!writers_.empty());
    }

    // Ends the current innermost frame, and prepares it for a fresh
    // frame.
    //
    // Invalidates any pending write buffer.
    INLINE void cycle_frame()
    {
        // Check for bad calls.
        assert(!writers_.empty());

        writers_.back().close(make_writer_state());
        last_buffer_size_ = -1;
    }

    // Closes the innermost frame
    //
    // Invalidates any pending write buffer.
    INLINE void pop_frame()
    {
        // Check for bad calls.
        assert(!writers_.empty());

        writers_.back().close(make_writer_state());
        writers_.pop_back();

        last_buffer_size_ = -1;
    }

    // Closes all frames.
    void pop_all_frames();

    // Returns a write buffer for the current innermost frame.
    //
    // It's always safe to write up to 64 bytes past the end of the return value.
    //
    // Invalidates any pending write buffer returned by earlier calls to `get_buffer()`.
    INLINE std::span<std::byte> get_buffer()
    {
        // Check for bad calls.
        assert(!writers_.empty());

        Buffer ret = writers_.back().get_buffer(make_writer_state());
        assert(!ret.write_buf.empty());

        last_buffer_size_ = ret.write_buf.size();
        return ret.write_buf;
    }

    // Commit `count` writes to the write buffer last returned by `get_buffer()`.
    //
    // Invalidates any pending write buffer.
    INLINE void commit(size_t count)
    {
        // Check for bad calls.
        assert(!writers_.empty());
        assert(last_buffer_size_ != size_t(-1) && count <= last_buffer_size_);

        writers_.back().commit(make_writer_state(), count);

        last_buffer_size_ = -1;
    }

    // Writes `frame_data` to the current innermost frame.
    //
    // If `force_write_if_empty` is true, opens a frame even if
    // `frame_data` is empty; otherwise, only ensures a toplevel frame
    // exist when there is data to write.
    //
    // Invalidates any pending write buffer.
    void write(std::span<const std::byte> frame_data, bool force_write_if_empty = false);

private:
    INLINE FrameWriterState make_writer_state()
    {
        return FrameWriterState{writers_.data(), provider_};
    }

    std::vector<BaseFrameWriter> writers_;
    // TODO: wrap `provider_` in a caching class, to amortize commit / refresh calls.
    BufferProvider *provider_ = nullptr;

    // Size of the last buffer returned by `get_buffer()`, or -1 if
    // that buffer (if any) has since been invalidated.
    size_t last_buffer_size_ = -1;
};
