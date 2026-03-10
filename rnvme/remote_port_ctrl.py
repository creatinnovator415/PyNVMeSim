import struct
import asyncio
import ctypes
import parse_rp_headers

# ============================================================
# Constants
# ============================================================

RP_CMD_nop        = 0
RP_CMD_hello      = 1
RP_CMD_cfg        = 2
RP_CMD_read       = 3
RP_CMD_write      = 4
RP_CMD_interrupt  = 5
RP_CMD_sync       = 6
RP_CMD_ats_req    = 7
RP_CMD_ats_inv    = 8

# MemTxResult
MEMTX_OK = 0
MEMTX_ERROR = 1
MEMTX_DECODE_ERROR = 2

# Response Status (from attributes)
RP_RESP_OK = 0
RP_RESP_ADDR_ERROR = 1
RP_RESP_DATA_ERROR = 2

RP_PKT_FLAGS_optional = 1 << 0
RP_PKT_FLAGS_response = 1 << 1
RP_PKT_FLAGS_posted   = 1 << 2

RP_BUS_ATTR_SECURE = 0x2

RP_VERSION_MAJOR = 4
RP_VERSION_MINOR = 3

class RemotePortCtrl:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.pkt_id = 0
        self.pending_responses = {} # pid -> asyncio.Future
        self.remote_port_proto = parse_rp_headers.remote_port_proto()

    def _get_next_id(self):
        pid = self.pkt_id
        self.pkt_id += 1
        return pid

    async def _wait_resp(self, pid):
        """Wait for a response packet with the given PID."""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.pending_responses[pid] = fut
        try:
            return await fut
        finally:
            self.pending_responses.pop(pid, None)

    def dispatch_response(self, pid, cmd, length, flags, dev, payload):
        if pid in self.pending_responses and not self.pending_responses[pid].done():
            self.pending_responses[pid].set_result((cmd, length, flags, dev, payload))

    async def rp_hello(self, dev=0, caps=None):
        if caps is None:
            caps = []
        
        cmd = RP_CMD_hello
        pid = self._get_next_id()
        flags = 0
        
        caps_payload = b""
        for cap in caps:
            caps_payload += struct.pack(">I", cap)
            
        # struct rp_pkt_hello size without header is 12 bytes
        # offset to caps data is sizeof(rp_pkt_hello) = 20 + 12 = 32
        caps_offset = 32 
        
        payload = struct.pack(">HH", RP_VERSION_MAJOR, RP_VERSION_MINOR)
        payload += struct.pack(">IHH", caps_offset, len(caps), 0)
        payload += caps_payload
        
        rp_header = struct.pack(">IIIII", cmd, len(payload), pid, flags, dev)
        
        self.writer.write(rp_header + payload)
        await self.writer.drain()

    async def rp_cfg(self, dev, opt, set_val):
        cmd = RP_CMD_cfg
        pid = self._get_next_id()
        flags = 0
        
        # struct rp_pkt_cfg { hdr, u32 opt, u8 set }
        payload = struct.pack(">IB", opt, set_val)
        
        rp_header = struct.pack(">IIIII", cmd, len(payload), pid, flags, dev)
        
        self.writer.write(rp_header + payload)
        await self.writer.drain()

    async def rp_interrupt(self, dev, line, val, vector=0, timestamp=0, posted=True):
        cmd = RP_CMD_interrupt
        pid = self._get_next_id()
        flags = RP_PKT_FLAGS_posted if posted else 0
        
        # struct rp_pkt_interrupt { hdr, u64 timestamp, u64 vector, u32 line, u8 val }
        payload_len = 21
        pkt = bytearray(20 + payload_len)

        self.rp_encode_hdr(pkt, cmd, pid, flags, dev, payload_len)
        struct.pack_into(">QQIB", pkt, 20, timestamp, vector, line, val)

        self.writer.write(pkt)
        await self.writer.drain()
        if not posted:
            await self._wait_resp(pid)

    async def rp_sync(self, dev, timestamp=0):
        cmd = RP_CMD_sync
        pid = self._get_next_id()
        flags = 0
        
        # struct rp_pkt_sync { hdr, u64 timestamp }
        payload = struct.pack(">Q", timestamp)
        
        rp_header = struct.pack(">IIIII", cmd, len(payload), pid, flags, dev)
        
        self.writer.write(rp_header + payload)
        await self.writer.drain()
        await self._wait_resp(pid)

    def rp_encode_busaccess_in_rsp_init(self, header, bus_access):
        """
        Initialize rp_encode_busaccess_in struct for response.
        header: tuple (cmd, len, id, flags, dev)
        bus_access: tuple (timestamp, addr, attr, len, width, stream_width, master_id)
        """
        cmd, length, pid, flags, dev = header
        ts, addr, attr, ln, w, sw, mid = bus_access

        in_struct = self.remote_port_proto.rp_encode_busaccess_in()
        in_struct.cmd = cmd
        in_struct.id = pid
        in_struct.flags = flags | RP_PKT_FLAGS_response
        in_struct.dev = dev
        in_struct.master_id = mid
        in_struct.addr = addr
        in_struct.size = ln
        in_struct.width = w
        in_struct.stream_width = sw
        in_struct.byte_enable_len = 0
        return in_struct

    def rp_dpkt_alloc(self, size):
        return bytearray(size)

    def rp_busaccess_tx_dataptr(self, pkt):
        # Header (20) + BusAccess (38) = 58
        return memoryview(pkt)[58:]

    def rp_normalized_vmclk(self):
        return 0

    def rp_encode_hdr(self, pkt, cmd, id, flags, dev, length):
        struct.pack_into(">IIIII", pkt, 0, cmd, length, id, flags, dev)
        return 20

    def rp_encode_busaccess_common(self, pkt, in_struct):
        hdr_len = 20
        clk = in_struct.clk if hasattr(in_struct, 'clk') else 0
        struct.pack_into(">QQQIIIH", pkt, hdr_len,
                         clk,
                         in_struct.attr,
                         in_struct.addr,
                         in_struct.size,
                         in_struct.width,
                         in_struct.stream_width,
                         in_struct.master_id)
        return 38

    def rp_encode_busaccess(self, pkt, in_struct):
        ba_len = 38
        payload_len = ba_len
        if in_struct.cmd == RP_CMD_write:
            payload_len += in_struct.size

        self.rp_encode_hdr(pkt, in_struct.cmd, in_struct.id, in_struct.flags, in_struct.dev, payload_len)
        self.rp_encode_busaccess_common(pkt, in_struct)

        return 20 + ba_len

    def rp_encode_read(self, pkt, id, dev, clk, master_id, addr, attr, size, width, stream_width):
        self.rp_encode_hdr(pkt, RP_CMD_read, id, 0, dev, 38)
        struct.pack_into(">QQQIIIH", pkt, 20, clk, attr, addr, size, width, stream_width, master_id)
        return 58

    def rp_encode_read_resp(self, pkt, id, dev, clk, master_id, addr, attr, size, width, stream_width):
        self.rp_encode_hdr(pkt, RP_CMD_read, id, RP_PKT_FLAGS_response, dev, 38 + size)
        struct.pack_into(">QQQIIIH", pkt, 20, clk, attr, addr, size, width, stream_width, master_id)
        return 58 + size

    def rp_encode_write(self, pkt, id, dev, clk, master_id, addr, attr, size, width, stream_width):
        self.rp_encode_hdr(pkt, RP_CMD_write, id, 0, dev, 38 + size)
        struct.pack_into(">QQQIIIH", pkt, 20, clk, attr, addr, size, width, stream_width, master_id)
        return 58 + size

    def rp_encode_write_resp(self, pkt, id, dev, clk, master_id, addr, attr, size, width, stream_width):
        self.rp_encode_hdr(pkt, RP_CMD_write, id, RP_PKT_FLAGS_response, dev, 38)
        struct.pack_into(">QQQIIIH", pkt, 20, clk, attr, addr, size, width, stream_width, master_id)
        return 58

    async def rp_write(self, data):
        self.writer.write(data)
        await self.writer.drain()

    def parse_bus_access(self, payload):
        # Base header size is 38 bytes.
        if len(payload) < 38:
            return None

        # Prepend 20 bytes for the header which is stripped in payload
        # and pad to ensure it's large enough for the extended struct.
        buf = (b'\x00' * 20 + payload).ljust(80, b'\x00')
        pkt = self.remote_port_proto.rp_pkt_busaccess_ext_base.from_buffer_copy(buf)

        addr = pkt.addr
        ln = pkt.len
        attr = pkt.attributes

        if attr & 0x4:
            if len(payload) < 60:
                return None
            header_len = pkt.data_offset - 20
        else:
            header_len = 38

        if header_len < 0 or len(payload) < header_len:
            return None

        data = payload[header_len : header_len + ln]
        return addr, ln, data, payload[:header_len]

    async def rp_mm_access(self, dev, addr, size, data=0, rw=False, attr=0, master_id=0, posted=False):
        """
        Perform a Memory Mapped access via Remote Port.
        Equivalent to rp_mm_access_with_def_attr in C.

        Args:
            dev (int): RP Device ID (e.g., RPDEV_PCI_BAR_BASE)
            addr (int): Address to access
            size (int): Size of access in bytes (1, 2, 4, 8, etc.)
            data (int/bytes): Data to write (if rw=True). Integer for <=8 bytes.
            rw (bool): True for Write, False for Read
            attr (int): Default attributes
            master_id (int): Requester ID
            posted (bool): If True, send as posted write (no response expected). Only valid for writes.

        Returns:
            (int, int/bytes): Tuple of (MemTxResult, read_data)
        """
        cmd = RP_CMD_write if rw else RP_CMD_read
        pid = self._get_next_id()
        flags = RP_PKT_FLAGS_posted if posted else 0
        
        in_struct = self.remote_port_proto.rp_encode_busaccess_in()
        in_struct.cmd = cmd
        in_struct.id = pid
        in_struct.flags = flags
        in_struct.dev = dev
        in_struct.clk = 0
        in_struct.master_id = master_id
        in_struct.addr = addr
        in_struct.attr = attr
        in_struct.size = size
        in_struct.width = 0
        in_struct.stream_width = size
        
        pkt_len = 20 + 38
        if rw:
            pkt_len += size
            
        pkt = bytearray(pkt_len)
        
        header_len = self.rp_encode_busaccess(pkt, in_struct)
        
        if rw:
            data_offset = header_len
            if isinstance(data, (bytes, bytearray, memoryview)):
                pkt[data_offset:data_offset+size] = data
            elif size == 1:
                struct.pack_into("<B", pkt, data_offset, data)
            elif size == 2:
                struct.pack_into("<H", pkt, data_offset, data)
            elif size == 4:
                struct.pack_into("<I", pkt, data_offset, data)
            elif size == 8:
                struct.pack_into("<Q", pkt, data_offset, data)
            else:
                raise ValueError(f"Data must be bytes/bytearray for size {size} > 8 or invalid integer size")

        self.writer.write(pkt)
        await self.writer.drain()
        if posted:
            return MEMTX_OK, 0
        r_cmd, r_len, r_flags, r_dev, r_payload = await self._wait_resp(pid)

        # Parse response
        # Base header is 38 bytes
        base_fmt = ">QQQIIIH"
        base_sz = struct.calcsize(base_fmt)

        if len(r_payload) < base_sz:
            return MEMTX_ERROR, 0

        r_ts, r_attr, r_addr, r_ln, r_w, r_sw, r_mid = struct.unpack(base_fmt, r_payload[:base_sz])
        
        # Check status from attributes (assuming lowest 2 bits hold status)
        status = r_attr & 0x3
        if status == RP_RESP_OK:
            ret = MEMTX_OK
        elif status == RP_RESP_ADDR_ERROR:
            ret = MEMTX_DECODE_ERROR
        else:
            ret = MEMTX_ERROR

        read_val = 0
        if not rw and ret == MEMTX_OK:
            # Determine header length based on attributes
            if r_attr & 0x4: # RP_BUS_ATTR_EXT_BASE
                if len(r_payload) < base_sz + 4:
                    return MEMTX_ERROR, 0
                data_offset = struct.unpack(">I", r_payload[base_sz:base_sz+4])[0]
                header_len = data_offset - 20
            else:
                header_len = base_sz

            if len(r_payload) < header_len:
                return MEMTX_ERROR, 0

            data_part = r_payload[header_len:]
            if size == 1:
                read_val = struct.unpack("<B", data_part)[0]
            elif size == 2:
                read_val = struct.unpack("<H", data_part)[0]
            elif size == 4:
                read_val = struct.unpack("<I", data_part)[0]
            elif size == 8:
                read_val = struct.unpack("<Q", data_part)[0]
            else:
                read_val = data_part

        return ret, read_val