#include <assert.h>
#include <array>
#include <cstdint>
#include <optional>
#include <vector>

#include "framing.h"

class FieldIndex
{
public:
    enum class Type : uint8_t
    {
        // Field indices 1 to 15
        idx1 = 0,
        idx15 = 14,

        ool_1,  // 1 byte out-of-line index
        ool_2,
        ool_3,
        ool_4,
        ool_5,

        sentinel,
    };

    INLINE FieldIndex()
        : bits_(encode(Type::sentinel, {0}, 0))
    {
    }

    INLINE constexpr explicit FieldIndex(uint64_t index)
    {
        assert(index > 0);  // XXX validation
        assert(index < (uint64_t(1) << 35));

        size_t num_bits = (8 * sizeof(long long)) - __builtin_clzll(index);
        size_t num_radix_128_digits = (num_bits + 6) / 7;

        Type ool_type = Type(uint8_t(Type::ool_1) - 1 + num_radix_128_digits);
        Type inline_type = Type(uint8_t(index - 1));

        Type type = (index <= 15) ? inline_type : ool_type;
        // TODO: pdep
        std::array<uint8_t, 5> out_of_line_bytes = {
            uint8_t((index >> 0) % 128),
            uint8_t((index >> 7) % 128),
            uint8_t((index >> 14) % 128),
            uint8_t((index >> 21) % 128),
            uint8_t((index >> 28) % 128),
        };

        bits_ = encode(type, out_of_line_bytes, (index <= 15) ? 0 : num_radix_128_digits);
    }

    INLINE Type get_type() const { return Type(bits_ >> 40); }
    // Number of out-of-line radix-128 digits for the field index.
    INLINE size_t num_radix_128_digits() const { return bits_ >> 48; }
    // Returns a pointer to 8 bytes, the first `num_radix_128_digits()` bytes of
    // which are the out of line index bytes.
    //
    // XXX: assume little endian
    INLINE const void *get_out_of_line_bytes() const { return &bits_; }

    // We always have 8 bytes to read.
    static constexpr inline size_t out_of_line_bytes_avail = 8;

private:
    static INLINE constexpr uint64_t encode(Type type, std::array<uint8_t, 5> index, size_t num_digits)
    {
        uint64_t ret = 0;

        // XXX: we assume little endian
        ret = uint64_t(index[0])
            + (uint64_t(index[1]) << 8)
            + (uint64_t(index[2]) << 16)
            + (uint64_t(index[3]) << 24)
            + (uint64_t(index[4]) << 32)
            + (uint64_t(type) << 40)
            + (uint64_t(num_digits) << 48);

        return ret;
    }

    // Store everything in a 64-bit value to make sure it's passed around in a single GPR.
    uint64_t bits_;
};

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
    // Up to 128 bytes of buffered data, plus 128 more for the new field, plus overflow bytes
    static inline constexpr size_t initial_data_capacity = 128 + 128 + 64;

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
    {
        assert(check_rep());
    }

    ~SubstreamFrame();

    // Move-only, the data can be pretty large.
    SubstreamFrame(const SubstreamFrame &) = delete;
    SubstreamFrame(SubstreamFrame &&) = default;
    SubstreamFrame &operator=(const SubstreamFrame &) = delete;
    SubstreamFrame &operator=(SubstreamFrame &&) = default;

    void close(ProducerContext context);
    INLINE bool is_closed() const { return closed_; }

    // Returns true if the field was successfully added, false otherwise.
    bool varlen_field(FieldIndex field, bool force_last = false);
    // TODO: ask for large buffer if we know the size.
    std::span<std::byte> get_buffer(ProducerContext context);
    void commit(ProducerContext context, size_t written);

    // Write to a varlen field.
    void write(ProducerContext context, std::span<const std::byte> field_data);
    void close_varlen_field();

    // Rest is only exposed for Producer::check_rep()
    INLINE bool is_verbatim() const { return !buffer_.has_value(); }

    bool is_large() const
    {
        if (!buffer_.has_value())
            return false;

        return buffer_.value().data.size() > initial_data_capacity;
    }

    bool check_rep() const;
    size_t get_nesting_level() const { return nesting_level_; }

private:
    // This is called when a nested child knows it'll exceed 127
    // bytes, and thus its parent and all surrounding substreams will too.
    INLINE void switch_to_verbatim(ProducerContext context)
    {
        assert(is_in_varlen_field_);  // XXX: input validation
        assert(!closed_);  // more validation

        assert(check_rep());

        if (!is_verbatim())
            switch_to_verbatim_impl(context);

        assert(check_rep());
    }

    void switch_to_verbatim_impl(ProducerContext context);

    struct Buffer
    {
        // Initialises `metadata` and `data` for a "small" buffering state.
        Buffer();

        std::vector<std::byte> metadata;  // Caps out at 252 + 8 bytes.  The extra 8 bytes allow sloppy writes.
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
    // This implicit conversion is only available at compile-time.
    struct ConstEvalFieldIndex
    {
        consteval ConstEvalFieldIndex(uint64_t index)  // NOLINT(google-explicit-constructor)
            : field(index)
        {}

        FieldIndex field;
    };

    explicit Producer(BufferProvider *provider)
        : writer_(provider)
    {
        assert(check_rep());
    }

    ~Producer();

    void push_substream(bool separate = false)
    {
        assert(check_rep());

        separate_substream(separate);
        substreams_.emplace_back(substreams_.size());

        assert(check_rep());
    }

    // Forces the current substream to start a new frame; this is
    // called automatically as needed, but an explicit call can
    // help readers skip ahead to interesting bits.
    void flush_substream()
    {
        assert(!substreams_.empty());  // XXX: usage validation

        assert(check_rep());

        substreams_.back().close(make_context());
        substreams_.back() = SubstreamFrame(substreams_.size() - 1);

        assert(check_rep());
    }

    void pop_substream(bool terminate = false)
    {
        assert(!substreams_.empty());

        assert(check_rep());

        substreams_.back().close(make_context());
        substreams_.pop_back();
        separate_substream(terminate);

        assert(check_rep());
    }

    // Inserts a 0-sized substream as a separator.
    INLINE void separate_substream(bool doit = true)
    {
        assert(check_rep());

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

        assert(check_rep());
    }

    void varlen_field(FieldIndex field, bool force_last = false)
    {
        assert(!substreams_.empty());  // XXX usage

        assert(check_rep());
        if (substreams_.back().varlen_field(field, force_last))
        {
            assert(check_rep());
            return;
        }

        assert(check_rep());

        // SLOW_PATH.
        flush_substream();
        bool success = substreams_.back().varlen_field(field, force_last);
        (void)success;
        assert(success);

        assert(check_rep());
    }

    // When `field` is a compile-time constants, allow implicit
    // conversion for convenience.
    INLINE void varlen_field(ConstEvalFieldIndex field, bool force_last = false)
    {
        varlen_field(field.field, force_last);
    }

    // TODO: cache the toplevel buffer to amortise commit calls
    // TODO: add refresh_buffer method.
    INLINE std::span<std::byte> get_buffer()
    {
        assert(!substreams_.empty());  // XXX usage

        assert(check_rep());
        std::span<std::byte> ret = substreams_.back().get_buffer(make_context());

        assert(!ret.empty());
        assert(check_rep());

        return ret;
    }

    INLINE void commit(size_t written)
    {
        assert(!substreams_.empty());  // XXX usage

        assert(check_rep());

        substreams_.back().commit(make_context(), written);

        assert(check_rep());
    }

    // Write to a varlen field.
    void write(std::span<const std::byte> data)
    {
        assert(!substreams_.empty());  // XXX usage

        assert(check_rep());
        substreams_.back().write(make_context(), data);
        assert(check_rep());
    }

    INLINE void close_varlen_field()
    {
        assert(!substreams_.empty());  // XXX usage

        assert(check_rep());
        substreams_.back().close_varlen_field();
        assert(check_rep());
    }

private:
    INLINE ProducerContext make_context() { return ProducerContext{substreams_.data(), &writer_}; }
    bool check_rep() const;

    std::vector<SubstreamFrame> substreams_;
    NestedFrameWriter writer_;
};
