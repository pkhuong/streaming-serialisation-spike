#include <assert.h>
#include <cstdint>
#include <optional>
#include <vector>

#include "framing.h"

class SubstreamFrame;

struct ProducerContext
{
    SubstreamFrame *frames = nullptr;
    NestedFrameWriter *writer = nullptr;
};

// A SubstreamFrame can be in one of three states:
//
//  - The initial state, waiting to see if it fits in 127 bytes.
//  - A large state, when we know it doesn't fit in 127 bytes.
//  - A verbatim state, when we know the frame exceeds 127 byte and
//    we're dumping the value for the last implicit field in the
//    frame.
//
// When a frame is in the "Large" state, all frames surrounding it are
// in the verbatim state, and all frames inside that large frame is in
// the initial state.
//
// When a frame is in the verbatim state, all frames surrounding it
// are also in the same verbatim state.
class SubstreamFrame
{
#ifndef SUBSTREAM_TEST_CONSTANTS
    static inline constexpr size_t max_inline_data_size = 127;

    static inline constexpr size_t max_metadata_size = 252;
#else
    // Must be less than the initial chunk size.
    static inline constexpr size_t max_inline_data_size = 4;

    // Less than the max inline size (do we strictly rely on that?)
    static inline constexpr size_t max_metadata_size = 3;
#endif

    // Worst case, we have a full chunk's worth of 2-byte inline metadata,
    // and the corresponding 127-byte data.
    static inline constexpr size_t max_data_size = (252 / 2) * 127;

public:
    explicit SubstreamFrame(size_t nesting_level)
        : buffer_(Buffer{})
        , nesting_level_(nesting_level)
    {}

    ~SubstreamFrame();

    // Move-only, the data can be pretty large.
    SubstreamFrame(const SubstreamFrame &) = delete;
    SubstreamFrame(SubstreamFrame &&) = default;
    SubstreamFrame &operator=(const SubstreamFrame &) = delete;
    SubstreamFrame &operator=(SubstreamFrame &&) = default;

    // Returns true if the field was successfully added, false otherwise.
    bool varlen_field(uint64_t field, bool force_last = false);
    // TODO: ask for large buffer if we know the size.
    std::span<std::byte> get_buffer(ProducerContext context);
    void commit(ProducerContext context, size_t written);

    // Write to a varlen field.
    void write(ProducerContext context, std::span<const std::byte> field_data);
    void close_varlen_field();

    void close(ProducerContext context);

    // This is called when a nested child knows it'll exceed 127
    // bytes, and thus its parent and all surrounding substreams will too.
    INLINE void switch_to_verbatim(ProducerContext context)
    {
        assert(is_in_varlen_field_);  // XXX: input validation
        assert(!closed_);  // more validation

        assert(this == &context.frames[nesting_level_]);

        if (!is_verbatim())
            switch_to_verbatim_impl(context);
    }

private:
    INLINE bool is_verbatim() const { return !buffer_.has_value(); }

    void switch_to_verbatim_impl(ProducerContext context);

    struct Buffer
    {
        // Initialises `metadata` and `data` for a "small" buffering state.
        Buffer();

        std::vector<std::byte> metadata;  // Caps out at 252 + 8 bytes
        std::vector<std::byte> data;

        uint32_t data_committed = 0;
        uint8_t metadata_written = 0;

        // This counter only has to go up to 128, and can saturate at
        // any value > 127 after that.
        //
        // Only valid while `is_in_varlen_field`.
        uint8_t current_field_size = 0;

        // True once we have an implicit length (or sentinel) at the
        // end of the metadata section.
        bool metadata_was_finalized = false;
    };

    // Empty when we're in the verbatim state.
    std::optional<Buffer> buffer_;
    uint32_t nesting_level_ = 0;
    bool is_in_varlen_field_ = false;
    bool closed_ = false;
};

class Producer
{
public:
    explicit Producer(BufferProvider *provider)
        : writer_(provider)
    {}

    ~Producer();

    void push_substream(bool separate = false)
    {
        if (separate)
            separate_substream();
        substreams_.emplace_back(substreams_.size());
    }

    // Forces the current substream to start a new frame; this is
    // called automatically as needed, but an explicit call can
    // help readers skip ahead to interesting bits.
    void flush_substream()
    {
        assert(!substreams_.empty());
        substreams_.back().close(make_context());
        substreams_.back() = SubstreamFrame(substreams_.size() - 1);
    }

    void pop_substream(bool terminate = false)
    {
        assert(!substreams_.empty());
        substreams_.back().close(make_context());
        substreams_.pop_back();

        if (terminate)
            separate_substream();
    }

    // Inserts a 0-sized substream as a separator.
    INLINE void separate_substream(bool doit = true)
    {
        if (__builtin_constant_p(doit) && !doit)
            return;

        if (substreams_.empty())
        {
            if (!doit)
                return;

            BufferProvider *provider = writer_.get_buffer_provider();

            assert(provider);
            const std::byte zero[] = { std::byte(0) };
            provider->write(zero);
        }
        else
        {
            std::span<std::byte> dst = get_buffer();

            assert(dst.size() >= 1);
            dst[0] = std::byte(0);
            commit(doit ? 1 : 0);
        }
    }

    void varlen_field(uint64_t field, bool force_last = false)
    {
        assert(!substreams_.empty());

        if (substreams_.back().varlen_field(field, force_last))
            return;

        // SLOW_PATH.
        flush_substream();
        bool success = substreams_.back().varlen_field(field, force_last);
        assert(success);
    }

    INLINE std::span<std::byte> get_buffer()
    {
        assert(!substreams_.empty());
        return substreams_.back().get_buffer(make_context());
    }

    INLINE void commit(size_t written)
    {
        assert(!substreams_.empty());
        return substreams_.back().commit(make_context(), written);
    }

    // Write to a varlen field.
    void write(std::span<const std::byte> data)
    {
        assert(!substreams_.empty());
        substreams_.back().write(make_context(), data);
    }

    INLINE void close_varlen_field()
    {
        assert(!substreams_.empty());
        return substreams_.back().close_varlen_field();
    }

private:
    INLINE ProducerContext make_context() { return ProducerContext{substreams_.data(), &writer_}; }

    std::vector<SubstreamFrame> substreams_;
    NestedFrameWriter writer_;
};
