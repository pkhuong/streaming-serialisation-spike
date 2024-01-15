#include "framing.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <string_view>
#include <vector>

class TestBufferProvider : public BufferProvider
{
public:
    static inline constexpr size_t max_buffer_size = 8;

#ifndef FRAME_TEST_CONSTANTS
    static inline constexpr size_t max_buffering = 2 + 253 * 253 - 1;
#else
    static inline constexpr size_t max_buffering = 13;
#endif

    explicit TestBufferProvider(size_t max_capacity)
    {
        buf_.resize(max_capacity + 64);
    }

    std::span<const std::byte> output() const
    {
        assert(backref_counter_ == 0);

        return std::span(buf_.data(), write_ptr_);
    }

    Buffer get_buffer() override
    {
        std::span<std::byte> dst = buf_;
        dst = dst.subspan(write_ptr_);

        size_t max_size = std::min(buf_.size() - 64 - write_ptr_, max_buffer_size);
        assert(max_size > 0 && "Regrowth not implemented");

        dst = dst.first(max_size);
        return Buffer{dst, write_ptr_, &backref_counter_, &next_backref_location_};
    }

    void commit(size_t count) override
    {
        size_t max_size = std::min(buf_.size() - 64 - write_ptr_, max_buffer_size);

        assert(count <= max_size);
        write_ptr_ += count;
    }

    void fill_backrefs(std::span<const std::pair<Backref, uint8_t>> target_values) override
    {
        size_t max_size = std::min(buf_.size() - 64 - write_ptr_, max_buffer_size);

        assert(next_backref_location_ <= write_ptr_ + max_size);
        for (const auto &[backref, value] : target_values)
        {
            assert(backref.is_valid());
            assert(backref.offset < next_backref_location_);
            assert(backref.offset >= write_ptr_ || write_ptr_ - backref.offset <= max_buffering);

            buf_[backref.offset] = std::byte(value);
            --backref_counter_;
        }
    }

private:
    std::vector<std::byte> buf_;
    size_t write_ptr_ = 0;

    uint32_t backref_counter_ = 0;
    uint64_t next_backref_location_ = 0;
};

int main()
{
    TestBufferProvider provider(2048);
    NestedFrameWriter writer(&provider);

    const auto &write = [&](std::string_view data)
    {
        writer.write(std::span(reinterpret_cast<const std::byte *>(data.data()), data.size()));
    };

    {
        writer.push_frame();

        write("12");

        {
            writer.push_frame();
            write("abcde");

            writer.cycle_frame();

            write("zsxcvbnm,./");

            writer.pop_frame();
        }

        write("34");
        writer.pop_frame();
    }

    for (const auto byte : provider.output())
    {
        printf("%c", uint8_t(byte));
    }

    return 0;
}
