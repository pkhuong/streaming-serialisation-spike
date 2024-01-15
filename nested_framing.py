"""Reference implementation for nested framing with bounded
(radix-253) chunks.
"""

class Buffer:

    class Backpatch:
        def __init__(self, buf, view):
            assert isinstance(view, memoryview)
            assert len(view) == 1
            self.buf = buf
            self.view = view

        def fill(self, value):
            self.view[0] = value
            self.buf._forget_backpatch(self)

    def __init__(self, backing_storage):
        self.backing_storage = backing_storage
        self.backpatches = set()
        self.written = 0
        self.closed = False

    def register_backpatch(self, view):
        """Returns a backpatch for the next byte in the write window."""
        assert not self.closed
        # TODO: assert view is in self.backing_storage
        ret = Buffer.Backpatch(self, view)
        self.backpatches.add(ret)
        return ret

    def _forget_backpatch(self, backpatch):
        assert backpatch in self.backpatches
        self.backpatches.remove(backpatch)

    def commit(self, written):
        assert not self.closed
        self.written += written
        assert self.written <= len(self.backing_storage)
        self.closed =  self.written == len(self.backing_storage)

    def close(self):
        assert not self.closed
        self.closed = True

    def get_destination(self):
        assert not self.closed
        assert 0 <= self.written < len(self.backing_storage)
        return memoryview(self.backing_storage)[self.written:]

    def get_buffered_bytes(self):
        assert self.closed
        return memoryview(self.backing_storage)[:self.written]


class BufferProvider:

    def __init__(self):
        self.buffers = []

    def register_backpatch(self, view):
        return self.buffers[-1].register_backpatch(view)

    def commit(self, written):
        self.buffers[-1].commit(written)

    def close(self):
        self.buffers[-1].close()

    def get_buffer(self, max_len):
        if not self.buffers or self.buffers[-1].closed:
            self.buffers.append(Buffer(bytearray(max(1, max_len))))
        return self.buffers[-1].get_destination()

    def get_data(self):
        assert all(buf.closed for buf in self.buffers)
        assert all(not(buf.backpatches) for buf in self.buffers)
        return b"".join(buf.get_buffered_bytes() for buf in self.buffers)


class FrameWriter:

    INITIAL_CHUNK_SIZE = 4 # 252
    FOLLOWUP_CHUNK_SIZE = 8 # 253 * 253 - 1

    def __init__(self, parent, buffer_provider):
        # Source of destination buffer.  Parent is none in toplevel frame writer.
        self.parent = parent
        self.buffer_provider = buffer_provider

        # How many chunks we've fully written out so far
        self.chunk_count = 0
        # Whether the previous chunk was maximally sized, in which case we must sent another one
        self.expect_followup = False

        # Current chunk state
        self.chunk_payload_size = 0  # Data bytes written in the current chunk
        self.header_backpatches = ()

        # Destination buffer state.  A chunk may be written out across
        # multiple destination buffers, and one destination buffer may
        # (partially) hold multiple chunks.
        self.destination_buffer = None  # memory view
        self.bytes_written_to_destination_buffer = None  # number of bytes already written in destination buffer

    # read-only helpers to track the current chunk size, with or without the header.
    def _max_chunk_size(self):
        """Returns the maximum size (including the size header) for the current chunk."""
        return 1 + FrameWriter.INITIAL_CHUNK_SIZE if self.chunk_count == 0 else 2 + FrameWriter.FOLLOWUP_CHUNK_SIZE

    def _max_payload_size(self):
        """Returns the maximum *data* size for the current chunk."""
        return FrameWriter.INITIAL_CHUNK_SIZE if self.chunk_count == 0 else FrameWriter.FOLLOWUP_CHUNK_SIZE

    # destination buffer management
    def _commit_to_parent(self):
        """Clears the current destination buffer, if any, and pushes
        the state back to the parent FrameWriter, if any.
        """
        if self.destination_buffer is not None:
            if self.parent is not None:
                self.parent.commit(self.bytes_written_to_destination_buffer)
            else:
                self.buffer_provider.commit(self.bytes_written_to_destination_buffer)
        self.destination_buffer = None
        self.bytes_written_to_destination_buffer = None

    def _ensure_destination_buffer(self, max_chunk_size):
        """Ensures `self.destination_buffer` exists and isn't exhausted.
        max_chunk_size is passed to `self.buffer_provider.get_buffer()` if
        we need a fresh destination buffer from the toplevel buffer provider.
        """
        if self.destination_buffer is not None and len(self.destination_buffer) > 0:
            return self.destination_buffer

        self._commit_to_parent()
        if self.parent is not None:
            self.destination_buffer = self.parent.get_write_buffer()
        else:
            self.destination_buffer = self.buffer_provider.get_buffer(max_chunk_size)

        assert len(self.destination_buffer) > 0
        self.bytes_written_to_destination_buffer = 0
        return self.destination_buffer

    def _advance_destination_buffer(self, count):
        assert count <= len(self.destination_buffer)
        self.bytes_written_to_destination_buffer += count
        self.destination_buffer = self.destination_buffer[count:]

    # chunk management
    def get_write_buffer(self):
        """Returns the current write buffer.  This write buffer always has room for at least one byte.

        This method must only be called when at least one byte will be (committed) to the buffer.
        """
        assert self.chunk_payload_size < self._max_payload_size()
        max_chunk_size = self._max_chunk_size()
        ret = self._ensure_destination_buffer(max_chunk_size)

        if not self.header_backpatches:  # Fresh chunk
            backpatch_count = 1 if self.chunk_count == 0 else 2
            backpatches = []
            for _ in range(backpatch_count):
                assert len(self.destination_buffer) >= 1
                backpatches.append(self.buffer_provider.register_backpatch(self.destination_buffer[:1]))
                self._advance_destination_buffer(1)
                # Make sure `ret` isn't empty
                ret = self._ensure_destination_buffer(max_chunk_size)

            self.header_backpatches = tuple(backpatches)

        assert self.chunk_payload_size < self._max_payload_size()
        return ret[:self._max_payload_size() - self.chunk_payload_size]

    def _finalize_current_chunk(self):
        """Fills the header backpatch byte(s) for the current chunk,
        and clears the current chunk state.
        """
        if self.chunk_count == 0:
            # Initial chunk.
            assert self.chunk_payload_size <= FrameWriter.INITIAL_CHUNK_SIZE
            assert len(self.header_backpatches) == 1
            self.header_backpatches[0].fill(self.chunk_payload_size)
            self.expect_followup = self.chunk_payload_size == FrameWriter.INITIAL_CHUNK_SIZE
        else:
            # Follow-up chunk
            assert self.chunk_payload_size <= FrameWriter.FOLLOWUP_CHUNK_SIZE
            assert len(self.header_backpatches) == 2
            self.header_backpatches[0].fill(self.chunk_payload_size % 253)
            self.header_backpatches[1].fill(self.chunk_payload_size // 253)
            self.expect_followup = self.chunk_payload_size == FrameWriter.FOLLOWUP_CHUNK_SIZE
        
        self.chunk_count += 1
        self.chunk_payload_size = 0
        self.header_backpatches = ()

    def commit(self, count):
        assert count >= 0
        if count == 0:
            return

        assert self.header_backpatches

        self._advance_destination_buffer(count)
        max_payload_size = self._max_payload_size();
        assert self.chunk_payload_size < max_payload_size
        self.chunk_payload_size += count
        assert self.chunk_payload_size <= max_payload_size

        if self.chunk_payload_size == max_payload_size:
            self._finalize_current_chunk()

    def close(self):
        if self.header_backpatches:
            self._finalize_current_chunk()

        if self.expect_followup:
            self.get_write_buffer()
            self._finalize_current_chunk()
        self._commit_to_parent()


class NestedWriter:

    def __init__(self, buffer_provider):
        self.buffer_provider = buffer_provider
        self.frame_writers = []

    def push(self):
        parent = self.frame_writers[-1] if self.frame_writers else None
        self.frame_writers.append(FrameWriter(parent, self.buffer_provider))

    def pop(self):
        assert self.frame_writers
        self.frame_writers[-1].close()
        self.frame_writers.pop()
        
    def get_write_buffer(self):
        return self.frame_writers[-1].get_write_buffer()

    def commit(self, count):
        self.frame_writers[-1].commit(count)

    def write(self, data):
        data = memoryview(data)
        while len(data) > 0:
            dst = self.get_write_buffer()
            assert len(dst) > 0
            to_write = min(len(dst), len(data))
            dst[:to_write] = data[:to_write]
            self.commit(to_write)
            data = data[to_write:]

    def insert_separator(self):
        self.push()
        self.get_write_buffer()  # This allocates a chunk
        self.pop()

    def close(self):
        assert not self.frame_writers
        self.buffer_provider.close()
        return self.buffer_provider.get_data()
