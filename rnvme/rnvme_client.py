#!/usr/bin/env python3
import asyncio
import struct
import sys
import os
import ctypes
import time
import parse_rp_headers
import socket
from remote_port_ctrl import RemotePortCtrl
import logging
# ============================================================
# Remote Port protocol constants
# ============================================================

RP_CMD_nop        = 0
RP_CMD_hello      = 1
RP_CMD_cfg        = 2
RP_CMD_read       = 3
RP_CMD_write      = 4
RP_CMD_interrupt  = 5
RP_CMD_sync       = 6

RP_PKT_FLAGS_optional = 0x1
RP_PKT_FLAGS_response = 0x2
RP_PKT_FLAGS_posted   = 0x4

# Response Status (from attributes)
RP_RESP_OK = 0

NVME_CMD_FLUSH = 0x00
NVME_CMD_WRITE = 0x01
NVME_CMD_READ  = 0x02
NVME_CMD_WRITE_ZEROES = 0x08
NVME_SUCCESS   = 0x0000
NVME_INVALID_OPCODE = 0x0001
NVME_LBA_RANGE = 0x0080
NVME_INVALID_FIELD = 0x0002
NVME_INVALID_NSID = 0x000b
NVME_DNR       = 0x4000

NVME_VOLATILE_WRITE_CACHE = 0x06
NVME_NUMBER_OF_QUEUES = 0x07
NVME_TIMESTAMP = 0x0E
NVME_ADM_CMD_DELETE_SQ      = 0x00
NVME_ADM_CMD_CREATE_SQ      = 0x01
NVME_ADM_CMD_GET_LOG_PAGE   = 0x02
NVME_ADM_CMD_DELETE_CQ      = 0x04
NVME_ADM_CMD_CREATE_CQ      = 0x05
NVME_ADM_CMD_IDENTIFY       = 0x06
NVME_ADM_CMD_ABORT          = 0x08
NVME_ADM_CMD_SET_FEATURES   = 0x09
NVME_ADM_CMD_GET_FEATURES   = 0x0a
NVME_ADM_CMD_ASYNC_EV_REQ   = 0x0c

# ============================================================
# RPDEV IDs (完整 Server 支援)
# ============================================================

RPDEV_PCI_CONFIG        = 0
RPDEV_PCI_LEGACY_IRQ    = 1
RPDEV_PCI_MESSAGES     = 2
RPDEV_PCI_DMA           = 3
RPDEV_PCI_BAR_BASE     = 10
RPDEV_NVME_CTRL        = 11
RPDEV_PCI_ATS          = 21

# ============================================================
# notifications
# ============================================================

NOTI_NVME_CTRL_CC       = 0
NOTI_MSIX               = 1
NOTI_INTMS              = 2
NOTI_INTMC              = 3
NOTI_NVME_CTRL_AQA      = 4
NOTI_NVME_CTRL_ASQ      = 5
NOTI_NVME_CTRL_ACQ      = 6
NOTI_SQ_DOORBELL_BASE   = 0x100
NOTI_CQ_DOORBELL_BASE   = 0x200

# ============================================================
# Logging
# ============================================================

LOG_ENABLED = True

def setup_logger(name, log_file, level=logging.INFO):
    """Function to setup as many loggers as you want"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(log_file, mode='w')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False
    return logger

if LOG_ENABLED:
    if not os.path.exists('logs'):
        os.makedirs('logs')
    log_main = setup_logger('main', 'logs/rnvme_client_main.log')
    log_nvme = setup_logger('nvme', 'logs/rnvme_nvme.log', level=logging.DEBUG)
    log_bus = setup_logger('bus', 'logs/rnvme_bus.log')
    log_sync = setup_logger('sync', 'logs/rnvme_sync.log')
else:
    # Create dummy loggers if logging is disabled
    log_main = log_nvme = log_bus = log_sync = logging.getLogger('dummy')
    log_main.addHandler(logging.NullHandler())

# ============================================================
# Global state
# ============================================================

BUS_FMT = ">QQQIIIHIIII"
BUS_SZ  = struct.calcsize(BUS_FMT)

class RnvmeClient:
    def __init__(self):
        self.pci_config = bytearray(4096)
        self.msix_mem   = bytearray(4096)
        self.remote_port_ctrl = None
        self.worker = {}

        # Storage simulation (128MB default)
        self.block_shift = 12 # 4KB
        self.ns_size_blocks = 0x200000 # 8GB (8 * 1024*1024*1024 / 4096)
        self.storage = bytearray(self.ns_size_blocks * (1 << self.block_shift))
        self.sqs = {}
        self.cqs = {}
        self.namespaces = []
        self.notifications = {}
        self._init_pci_config()


        self.header = parse_rp_headers.remote_port_header()

        # Create a controller instance using the header parser's factory so
        # nested fields (including anonymous inner structs) are initialized.
        try:
            self.ctrl = self.header.make_struct_instance('RPCNvmeCtrl')
        except Exception:
            # Fall back to direct instantiation if factory not available
            ctrl_cls = getattr(self.header, 'RPCNvmeCtrl', None) or self.header.parser.get_struct('RPCNvmeCtrl')
            if ctrl_cls is None:
                raise RuntimeError('RPCNvmeCtrl struct not found in parsed headers')
            self.ctrl = ctrl_cls()

        # If the factory didn't create admin queues, ensure they exist.
        if getattr(self.ctrl, 'admin_cq', None) is None:
            cqueue_cls = getattr(self.header, 'RPCNvmeCQueue', None) or self.header.parser.get_struct('RPCNvmeCQueue')
            if cqueue_cls:
                try:
                    self.ctrl.admin_cq = cqueue_cls()
                except Exception:
                    pass
        if getattr(self.ctrl, 'admin_sq', None) is None:
            squeue_cls = getattr(self.header, 'RPCNvmeSQueue', None) or self.header.parser.get_struct('RPCNvmeSQueue')
            if squeue_cls:
                try:
                    self.ctrl.admin_sq = squeue_cls()
                except Exception:
                    pass

        # Initialize admin CQ phase tag (best-effort)
        try:
            self.ctrl.admin_cq.phase = 1
        except Exception:
            if not getattr(self.ctrl, 'admin_cq', None):
                class _TinyCQ:
                    def __init__(self):
                        self.phase = 1
                        self.head = 0
                        self.size = 0
                self.ctrl.admin_cq = _TinyCQ()
        self._init_nvme_bar()
        self._init_id_ctrl()
        self._init_namespaces()
        self.ctrl.num_queues = 64
        
    def register_notifications(self, key, cb):
        self.notifications[key] = cb
        
    def get_notifications(self, key):
        return self.notifications.get(key)
        
    def trigger_notifications(self, key, *args):
        try:
            cb = self.notifications.get(key)
            if cb:
                cb(*args)
        except Exception as e:
            log_main.error("Exception in trigger_notifications for key %s: %s", key, e, exc_info=True)
                
    def _init_pci_config(self):
        struct.pack_into("<H", self.pci_config, 0x00, 0x8086)
        struct.pack_into("<H", self.pci_config, 0x02, 0x5845)
        self.pci_config[0x09] = 0x01
        self.pci_config[0x0A] = 0x08
        self.pci_config[0x0B] = 0x02
        self.pci_config[0x34] = 0x80
        struct.pack_into("<H", self.pci_config, 0x2C, 0x8086) # Subsystem Vendor ID
        struct.pack_into("<I", self.pci_config, 0x10, 0xFFFFC000)
        # BAR 4 for MSI-X (4KB)
        struct.pack_into("<I", self.pci_config, 0x20, 0xFFFFF000)

        # MSI-X Capability at 0x80
        # ID=0x11, Next=0x00, Ctrl=0x003F (Table Size=64)
        struct.pack_into("<B", self.pci_config, 0x80, 0x11)
        struct.pack_into("<B", self.pci_config, 0x81, 0x00)
        struct.pack_into("<H", self.pci_config, 0x82, 0x803F)
        # Table Offset=0x0000, BIR=4 (BAR4)
        struct.pack_into("<I", self.pci_config, 0x84, 0x0000 | 4)
        # PBA Offset=0x0800, BIR=4 (BAR4)
        struct.pack_into("<I", self.pci_config, 0x88, 0x0800 | 4)

    def _init_nvme_bar(self):
        self.ctrl.bar.cap = 0x7FF | (1 << 16) | (1 << 24)
        self.ctrl.bar.vs = 0x00010200
        self.ctrl.bar.cc = 0
        # From NvmeIdCtrl
        self.id_ctrl_sqes = (6 << 4) | 6
        self.id_ctrl_cqes = (4 << 4) | 4
        self.ctrl.bar.csts = 0
        self.ctrl.irq_status = 0

    def _init_id_ctrl(self):
        if not hasattr(self.ctrl, 'id_ctrl'):
            return

        id_ctrl = self.ctrl.id_ctrl
        
        # VID/SSVID from PCI Config
        id_ctrl.vid = struct.unpack_from("<H", self.pci_config, 0x00)[0]
        id_ctrl.ssvid = struct.unpack_from("<H", self.pci_config, 0x2C)[0]
        
        def set_str(field, s):
            val = s.encode('ascii')
            size = len(field)
            val = val.ljust(size, b' ')
            for i in range(size):
                field[i] = val[i]

        set_str(id_ctrl.sn, "PYTHON_NVME")
        set_str(id_ctrl.mn, "QEMU NVMe Ctrl")
        set_str(id_ctrl.fr, "1.0")
        
        id_ctrl.rab = 6
        id_ctrl.ieee[0] = 0x00
        id_ctrl.ieee[1] = 0x02
        id_ctrl.ieee[2] = 0xb3
        id_ctrl.cmic = 0
        id_ctrl.mdts = 7
        id_ctrl.sqes = (6 << 4) | 6
        id_ctrl.cqes = (4 << 4) | 4
        id_ctrl.nn = 1
        id_ctrl.oncs = 0x58
        id_ctrl.ver = 0x00010400
        id_ctrl.cntlid = 1
        id_ctrl.aerl = 3
        id_ctrl.oacs = 0x0A

    def _init_namespaces(self):
        if not hasattr(self.header, 'parser'):
            log_nvme.warning("Header parser not available, cannot initialize NvmeIdNs.")
            return

        num_ns = self.ctrl.id_ctrl.nn
        if num_ns == 0:
            return

        try:
            ns_cls = self.header.parser.get_struct('RPCNvmeNamespace')
        except (AttributeError, KeyError):
            log_nvme.warning("RPCNvmeNamespace struct not found, cannot pre-initialize namespace.")
            return

        self.namespaces = []
        ns_size_blocks_per_ns = self.ns_size_blocks // num_ns

        for i in range(num_ns):
            ns = ns_cls()
            id_ns = ns.id_ns

            id_ns.nsfeat = 0
            id_ns.nlbaf = 0
            id_ns.flbas = 0
            id_ns.mc = 0
            id_ns.dpc = 0
            id_ns.dps = 0
            id_ns.lbaf[0].ds = self.block_shift

            id_ns.ncap = ns_size_blocks_per_ns
            id_ns.nuse = ns_size_blocks_per_ns
            id_ns.nsze = ns_size_blocks_per_ns

            # EUI64 and NGUID are Big Endian in NVMe spec.
            # Since we are using LittleEndianStructure, we need to swap bytes for scalar fields
            # to ensure they appear as Big Endian in memory.
            eui64_val = 0x5254000000000000 + (i + 1)
            if hasattr(id_ns, 'eui64'):
                id_ns.eui64 = struct.unpack("<Q", eui64_val.to_bytes(8, 'big'))[0]

            if hasattr(id_ns, 'nguid'):
                nguid_bytes = eui64_val.to_bytes(8, 'big') * 2
                for j in range(16):
                    id_ns.nguid[j] = nguid_bytes[j]

            self.namespaces.append(ns)

    def rpc_nvme_init_cq(self, cq, n, dma_addr, cqid, vector, size, irq_enabled):
        cq.ctrl = ctypes.addressof(n)
        cq.cqid = cqid
        cq.size = size
        cq.dma_addr = dma_addr
        cq.phase = 1
        cq.irq_enabled = irq_enabled
        cq.vector = vector
        cq.head = 0
        cq.tail = 0
        if hasattr(cq, 'req_list'):
            cq.req_list.tql_next = 0
            cq.req_list.tql_prev = ctypes.addressof(cq.req_list)
        if hasattr(cq, 'sq_list'):
            cq.sq_list.tql_next = 0
            cq.sq_list.tql_prev = ctypes.addressof(cq.sq_list)

    def rpc_nvme_init_sq(self, sq, n, dma_addr, sqid, cqid, size):
        sq.ctrl = ctypes.addressof(n)
        sq.dma_addr = dma_addr
        sq.sqid = sqid
        sq.size = size
        sq.cqid = cqid
        sq.head = 0
        sq.tail = 0
        if hasattr(self.header, 'RPCNvmeRequest'):
            req_cls = self.header.RPCNvmeRequest
            req_array = (req_cls * size)()
            sq._io_req_keepalive = req_array
            sq.io_req = ctypes.addressof(req_array)
            for i in range(size):
                req_array[i].sq = ctypes.addressof(sq)
        if hasattr(sq, 'req_list'):
            sq.req_list.tql_next = 0
            sq.req_list.tql_prev = ctypes.addressof(sq.req_list)
        if hasattr(sq, 'out_req_list'):
            sq.out_req_list.tql_next = 0
            sq.out_req_list.tql_prev = ctypes.addressof(sq.out_req_list)

    async def connect(self, path_or_host, port=None, max_retries=5, retry_interval=10):
        retries = 0
        while True:
            try:
                # 1. Standard TCP Connection
                if port is not None:
                    reader, writer = await asyncio.open_connection(path_or_host, port)
                    self.remote_port_ctrl = RemotePortCtrl(reader, writer)
                    return

                # 2. Windows AF_UNIX Socket Connection (via WinSock2)
                if sys.platform == "win32" and port is None:
                    # 1. Setup the socket via WinSock2
                    AF_UNIX = 1
                    SOCK_STREAM = 1

                    class sockaddr_un(ctypes.Structure):
                        _fields_ = [("sun_family", ctypes.c_ushort),
                                    ("sun_path", ctypes.c_char * 108)]

                    ws2 = ctypes.windll.ws2_32
                    
                    # Create the socket
                    fd = ws2.socket(AF_UNIX, SOCK_STREAM, 0)
                    if fd == -1: # INVALID_SOCKET
                        raise OSError("Could not create AF_UNIX socket on Windows")

                    addr = sockaddr_un()
                    addr.sun_family = AF_UNIX
                    addr.sun_path = path_or_host.encode('utf-8')

                    # 2. Run the blocking connect in a thread to avoid freezing the loop
                    def _sync_connect():
                        result = ws2.connect(fd, ctypes.byref(addr), ctypes.sizeof(addr))
                        if result != 0:
                            last_error = ws2.WSAGetLastError()
                            raise OSError(f"Connect failed with WinSock error: {last_error}")
                        return socket.socket(family=AF_UNIX, type=socket.SOCK_STREAM, fileno=fd)

                    sock = await asyncio.to_thread(_sync_connect)
                    sock.setblocking(False)
                    reader, writer = await asyncio.open_connection(sock=sock)
                    self.remote_port_ctrl = RemotePortCtrl(reader, writer)
                    return

                # 3. Unix Domain Socket Connection
                reader, writer = await asyncio.open_unix_connection(path_or_host)
                self.remote_port_ctrl = RemotePortCtrl(reader, writer)
                return
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    log_main.error("Failed to connect after %d attempts.", max_retries)
                    log_main.error("Exception: %s", e)
                    sys.exit(1)
                
                log_main.warning("Connection failed: %s. Retrying in %d seconds... (%d/%d)",
                                 e, retry_interval, retries, max_retries)
                await asyncio.sleep(retry_interval)

    async def send_pkt(self, cmd, dev, payload=b"", flags=0, pid=None):
        if pid is None:
            pid = self.remote_port_ctrl._get_next_id()
        hdr = struct.pack(">IIIII", cmd, len(payload), pid, flags, dev)
        self.remote_port_ctrl.writer.write(hdr + payload)
        await self.remote_port_ctrl.writer.drain()
        
        log_msg = f"SEND cmd={cmd} dev={dev} len={len(payload)} flags=0x{flags:x} pid={pid}"
        if cmd == RP_CMD_sync:
            log_sync.info(log_msg)
        elif cmd in [RP_CMD_read, RP_CMD_write]:
            log_bus.info(log_msg)
        elif dev in [RPDEV_PCI_BAR_BASE, RPDEV_NVME_CTRL, RPDEV_PCI_CONFIG]:
            log_nvme.info(log_msg)
        else:
            log_main.info(log_msg)

    async def rpc_nvme_irq_check(self):
        msix_ctrl = struct.unpack("<H", self.pci_config[0x82:0x84])[0]
        if msix_ctrl & 0x8000: # msix_enabled
            await self.remote_port_ctrl.rp_interrupt(RPDEV_PCI_MESSAGES, 0, 0, 0, posted=False)
            return

        if (~self.ctrl.bar.intms & self.ctrl.irq_status) != 0:
            await self.remote_port_ctrl.rp_interrupt(RPDEV_PCI_LEGACY_IRQ, 0, 1, 0, posted=False)
        else:
            await self.remote_port_ctrl.rp_interrupt(RPDEV_PCI_LEGACY_IRQ, 0, 0, 0, posted=False)

    async def rpc_nvme_irq_assert(self, cq):
        if cq.irq_enabled:
            msix_ctrl = struct.unpack("<H", self.pci_config[0x82:0x84])[0]
            if msix_ctrl & 0x8000: # msix_enabled
                log_nvme.info("IRQ (MSI-X) notify vector %d", cq.vector)
                await self.remote_port_ctrl.rp_interrupt(RPDEV_PCI_MESSAGES, 0, 1, vector=cq.vector, posted=False)
            else: # legacy INTx
                log_nvme.info("IRQ (Legacy) assert pin")
                assert cq.cqid < 64
                self.ctrl.irq_status |= (1 << cq.cqid)
                await self.rpc_nvme_irq_check()
        else:
            log_nvme.info("IRQ masked")

    async def rpc_nvme_irq_deassert(self, cq):
        if cq.irq_enabled:
            msix_ctrl = struct.unpack("<H", self.pci_config[0x82:0x84])[0]
            if not (msix_ctrl & 0x8000): # not msix_enabled
                assert cq.cqid < 64
                self.ctrl.irq_status &= ~(1 << cq.cqid)
                await self.rpc_nvme_irq_check()

    async def rpc_nvme_msix_assert(self, cq):
        log_nvme.info("IRQ (MSI-X) notify vector %d assert", cq)
        await self.remote_port_ctrl.rp_interrupt(RPDEV_PCI_MESSAGES, 0, 1, vector=cq, posted=False)
        
    async def send_nvme_irq(self, cq):
        await self.rpc_nvme_irq_assert(cq)

    def _bar0_read(self, addr, ln):
        bar_size = ctypes.sizeof(self.ctrl.bar)
        if addr >= bar_size:
            log_nvme.warning("BAR0 Read out of bounds: addr=0x%x, len=%d", addr, ln)
            return b'\x00' * ln
        
        bar_bytes = bytes(self.ctrl.bar)
        end = addr + ln
        read_data = bar_bytes[addr:end].ljust(ln, b'\x00')
        log_nvme.debug("BAR0 Read: addr=0x%x, len=%d, data=%s", addr, ln, read_data.hex())
        return read_data

    def _start_ctrl(self):
        log_nvme.info("Starting controller...")
        cc = self.ctrl.bar.cc
        cap = self.ctrl.bar.cap
        aqa = self.ctrl.bar.aqa
        asq = self.ctrl.bar.asq
        acq = self.ctrl.bar.acq

        page_bits = ((cc >> 7) & 0xf) + 12
        page_size = 1 << page_bits

        if asq == 0 or acq == 0:
            log_nvme.error("ASQ or ACQ is zero")
            return -1

        if asq & (page_size - 1) != 0 or acq & (page_size - 1) != 0:
            log_nvme.error("ASQ or ACQ not page aligned")
            return -1

        mpsmin = (cap >> 48) & 0xf
        mpsmax = (cap >> 52) & 0xf
        cc_mps = (cc >> 7) & 0xf
        if cc_mps < mpsmin or cc_mps > mpsmax:
            log_nvme.error("MPS %d out of range [%d-%d]", cc_mps, mpsmin, mpsmax)
            return -1

        id_sqes_min = self.id_ctrl_sqes & 0xf
        id_sqes_max = (self.id_ctrl_sqes >> 4) & 0xf
        id_cqes_min = self.id_ctrl_cqes & 0xf
        id_cqes_max = (self.id_ctrl_cqes >> 4) & 0xf

        iosqes = (cc >> 16) & 0xf
        iocqes = (cc >> 20) & 0xf
        if iosqes < id_sqes_min or iosqes > id_sqes_max:
            log_nvme.error("IOSQES %d out of range [%d-%d]", iosqes, id_sqes_min, id_sqes_max)
            return -1
        if iocqes < id_cqes_min or iocqes > id_cqes_max:
            log_nvme.error("IOCQES %d out of range [%d-%d]", iocqes, id_cqes_min, id_cqes_max)
            return -1

        asqs = aqa & 0xfff
        acqs = (aqa >> 16) & 0xfff
        if asqs == 0 or acqs == 0:
            log_nvme.error("AQA size is zero")
            return -1

        self.ctrl.page_bits = page_bits
        self.ctrl.page_size = page_size
        self.ctrl.max_prp_ents = self.ctrl.page_size // 8
        self.ctrl.cqe_size = 1 << iocqes
        self.ctrl.sqe_size = 1 << iosqes

        # Reset admin queues state
        self.rpc_nvme_init_cq(self.ctrl.admin_cq, self.ctrl, acq, 0, 0, acqs + 1, 1)
        self.rpc_nvme_init_sq(self.ctrl.admin_sq, self.ctrl, asq, 0, 0, asqs + 1)

        log_nvme.info("Controller started successfully")
        return 0

    def _clear_ctrl(self):
        log_nvme.info("Clearing controller state")
        self.sq_head = 0
        self.cq_head = 0
        self.cq_phase = 1
        self.admin_sq_size = 0
        self.admin_cq_size = 0

        self.ctrl.admin_sq.size = 0
        self.ctrl.admin_cq.size = 0
        self.ctrl.admin_sq.head = 0
        self.ctrl.admin_cq.head = 0

        self.ctrl.bar.cc = 0

    def _bar0_write(self, addr, data):
        size = len(data)
        val = 0
        if size == 4:
            val = struct.unpack("<I", data)[0]
        elif size == 8:
            val = struct.unpack("<Q", data)[0]

        worker_to_start = None
        cq_to_deassert = None

        log_nvme.debug("BAR0 Write: addr=0x%x, size=%d, val=0x%x", addr, size, val)

        if addr >= 0x2000:
            if self.get_notifications(NOTI_MSIX):
                self.trigger_notifications(NOTI_MSIX, addr, data)
            else:
                if addr + len(data) <= 0x2000 + len(self.msix_mem):
                    offset = addr - 0x2000
                    self.msix_mem[offset:offset + len(data)] = data
            return worker_to_start, cq_to_deassert

        if addr >= 0x1000:
            qid = (addr - 0x1000) >> 3
            if (addr - 0x1000) & 4: # CQ doorbell
                doorbell_val = struct.unpack("<I", data)[0]
                if self.get_notifications(NOTI_CQ_DOORBELL_BASE + qid):
                    self.trigger_notifications(NOTI_CQ_DOORBELL_BASE + qid, qid, doorbell_val)
                else:
                    if qid == 0:
                        self.ctrl.admin_cq.tail = doorbell_val
                        if self.ctrl.admin_cq.tail == self.ctrl.admin_cq.head:
                            cq_to_deassert = self.ctrl.admin_cq
                    else:
                        cq = self.cqs.get(qid)
                        if cq:
                            cq.tail = doorbell_val
                            if cq.tail == cq.head:
                                cq_to_deassert = cq
                        else:
                            log_nvme.warning("CQ%d doorbell write: CQ not found", qid)
            else: # SQ doorbell
                doorbell_val = struct.unpack("<I", data)[0]
                if self.get_notifications(NOTI_SQ_DOORBELL_BASE + qid):
                    self.trigger_notifications(NOTI_SQ_DOORBELL_BASE + qid, qid, doorbell_val)
                else:
                    if qid == 0:
                        self.ctrl.admin_sq.tail = doorbell_val
                        worker_to_start = ('admin', 0)
                    else:
                        sq = self.sqs.get(qid)
                        if sq:
                            sq.tail = doorbell_val
                            worker_to_start = ('io', qid)
                        else:
                            log_nvme.warning("IO SQ%d doorbell write: SQ not found", qid)
            return worker_to_start, cq_to_deassert

        # NVMe Registers
        if addr == 0x0c: # INTMS
            if size < 4: return worker_to_start, cq_to_deassert
            if self.get_notifications(NOTI_INTMS):
                self.trigger_notifications(NOTI_INTMS, val)
            else:
                self.ctrl.bar.intms |= (val & 0xffffffff)
                self.ctrl.bar.intmc = self.ctrl.bar.intms
        
        elif addr == 0x10: # INTMC
            if size < 4: return worker_to_start, cq_to_deassert
            if self.get_notifications(NOTI_INTMC):
                self.trigger_notifications(NOTI_INTMC, val)
            else:
                self.ctrl.bar.intms &= ~(val & 0xffffffff)
                self.ctrl.bar.intmc = self.ctrl.bar.intms
            
        elif addr == 0x14: # CC
            if size < 4: return worker_to_start, cq_to_deassert
            if self.get_notifications(NOTI_NVME_CTRL_CC):
                self.trigger_notifications(NOTI_NVME_CTRL_CC, val)
            else:
                old_cc = self.ctrl.bar.cc
                
                def NVME_CC_EN(v): return (v & 1)
                def NVME_CC_SHN(v): return (v >> 14) & 3
                
                if not NVME_CC_EN(val) and not NVME_CC_EN(old_cc) and \
                   not NVME_CC_SHN(val) and not NVME_CC_SHN(old_cc):
                    self.ctrl.bar.cc = val

                if NVME_CC_EN(val) and not NVME_CC_EN(old_cc):
                    self.ctrl.bar.cc = val
                    if self._start_ctrl() != 0:
                        log_nvme.error("start_ctrl failed")
                        self.ctrl.bar.csts |= (1 << 1) # CFS
                    else:
                        log_nvme.info("start_ctrl success")
                        self.ctrl.bar.csts |= 1 # RDY
                elif not NVME_CC_EN(val) and NVME_CC_EN(old_cc):
                    log_nvme.info("CC.EN 1->0 detected, disabling controller")
                    self._clear_ctrl()
                    self.ctrl.bar.csts &= ~1 # Clear RDY
                    self.ctrl.bar.cc = val

                if NVME_CC_SHN(val) and not NVME_CC_SHN(old_cc):
                    log_nvme.info("Shutdown started")
                    self._clear_ctrl()
                    self.ctrl.bar.cc = val
                    self.ctrl.bar.csts |= (2 << 2) # SHST_COMPLETE
                elif not NVME_CC_SHN(val) and NVME_CC_SHN(old_cc):
                    log_nvme.info("Shutdown cleared")
                    self.ctrl.bar.csts &= ~(3 << 2) # SHST clear
                    self.ctrl.bar.cc = val

        elif addr == 0x24: # AQA
            if self.get_notifications(NOTI_NVME_CTRL_AQA):
                self.trigger_notifications(NOTI_NVME_CTRL_AQA, val)
            else:
                self.ctrl.bar.aqa = val & 0xffffffff
        elif addr == 0x28: # ASQ
            if self.get_notifications(NOTI_NVME_CTRL_ASQ):
                self.trigger_notifications(NOTI_NVME_CTRL_ASQ, val)
            else:
                self.ctrl.bar.asq = val
        elif addr == 0x2c: # ASQ Hi
            if self.get_notifications(NOTI_NVME_CTRL_ASQ):
                self.trigger_notifications(NOTI_NVME_CTRL_ASQ, val)
            else:
                self.ctrl.bar.asq |= (val << 32)
        elif addr == 0x30: # ACQ
            if self.get_notifications(NOTI_NVME_CTRL_ACQ):
                self.trigger_notifications(NOTI_NVME_CTRL_ACQ, val)
            else:
                self.ctrl.bar.acq = val
        elif addr == 0x34: # ACQ Hi
            if self.get_notifications(NOTI_NVME_CTRL_ACQ):
                self.trigger_notifications(NOTI_NVME_CTRL_ACQ, val)
            else:
                self.ctrl.bar.acq |= (val << 32)

        return worker_to_start, cq_to_deassert

    def rpc_nvme_rp_io_write(self, pkt_header, pkt_payload):
        """
        Handles a Remote Port I/O write packet and prepares a response.
        This is a Python translation of the C function rpc_nvme_rp_io_write.
        Args:
            pkt_header (tuple): The unpacked packet header (cmd, length, pid, flags, dev).
            pkt_payload (bytes): The byte payload of the packet.
        Returns:
            A tuple (response_info, worker_to_start, cq_to_deassert). `response_info` is a
            (payload, flags) tuple or None.
        """
        cmd, length, pid, flags, dev = pkt_header

        # 1. Parse bus access information
        parsed = self.remote_port_ctrl.parse_bus_access(pkt_payload)
        if not parsed:
            return None, False, None

        addr, ln, data, header_bytes = parsed
        # Re-unpack base header to get fields needed for response (ts, attr, w, sw, mid)
        ts, attr, _, _, w, sw, mid = struct.unpack(">QQQIIIH", header_bytes[:38])

        # 2. Perform the actual MMIO write operation using the controller instance
        worker_to_start, cq_to_deassert = self._bar0_write(addr, data[:ln])

        # 3. If the packet is not "posted", prepare a response packet
        response_info = None
        if not (flags & RP_PKT_FLAGS_posted):
            # The response to a write contains a bus access header with a status,
            # but no data payload. The 'len' field in the response should be 0.
            resp_attr = attr | RP_RESP_OK
            resp_ts = self.remote_port_ctrl.rp_normalized_vmclk() + ts

            response_payload = struct.pack(">QQQIIIH", resp_ts, resp_attr, addr, 0, 0, sw, mid)
            response_flags = flags | RP_PKT_FLAGS_response
            response_info = (response_payload, response_flags)
            
        return response_info, worker_to_start, cq_to_deassert

    async def _dma_rw_prp(self, prp1, prp2, length, is_write, data=None):
        page_size = self.ctrl.page_size
        transferred = 0
        access_tasks = []
        
        # Helper to handle a single chunk
        async def _access_chunk(addr, size, chunk_data=None):
            if is_write:
                # Read from Host (DMA Read), Write to Storage
                _, buf = await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, addr, size, rw=False)
                if isinstance(buf, int): buf = buf.to_bytes(size, 'little')
                return buf
            else:
                # Read from Storage, Write to Host (DMA Write)
                await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, addr, size, data=chunk_data, rw=True)
                return None

        # 1. First Page
        offset = prp1 & (page_size - 1)
        chunk_len = min(length, page_size - offset)
        
        access_tasks.append(asyncio.create_task(_access_chunk(prp1, chunk_len, data[:chunk_len] if not is_write else None)))
        transferred += chunk_len

        # 2. Second Page or PRP List
        if transferred < length:
            if length - transferred <= page_size:
                # Just one more page, prp2 is the address
                chunk_len = length - transferred
                access_tasks.append(asyncio.create_task(_access_chunk(prp2, chunk_len, data[transferred:] if not is_write else None)))
            else:
                # prp2 is a pointer to a PRP List
                prp_list_addr = prp2
                entries_per_page = page_size // 8
                
                while transferred < length:
                    # Read PRP List Page
                    _, list_bytes = await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, prp_list_addr, page_size, rw=False)
                    
                    for i in range(entries_per_page):
                        if transferred >= length: break
                        
                        entry = struct.unpack("<Q", list_bytes[i*8:(i+1)*8])[0]
                        
                        # Check for chaining (last entry in page points to next list)
                        # Only if we still have more than 1 page worth of data remaining
                        if i == entries_per_page - 1 and (length - transferred) > page_size:
                            prp_list_addr = entry
                            break # Continue outer loop to read new list
                        
                        chunk_len = min(length - transferred, page_size)
                        chunk_data = data[transferred:transferred+chunk_len] if not is_write else None
                        access_tasks.append(asyncio.create_task(_access_chunk(entry, chunk_len, chunk_data)))
                        transferred += chunk_len

        results = await asyncio.gather(*access_tasks)
        if is_write:
            return b"".join(results)
        return None

    async def rpc_nvme_rw(self, cmd_bytes):
        # Parse NvmeRwCmd
        # 0: opcode, 4: nsid, 24: prp1, 32: prp2, 40: slba, 48: nlb
        dw0 = struct.unpack("<I", cmd_bytes[0:4])[0]
        opcode = dw0 & 0xFF
        
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        prp2 = struct.unpack("<Q", cmd_bytes[32:40])[0]
        slba = struct.unpack("<Q", cmd_bytes[40:48])[0]
        nlb  = (struct.unpack("<H", cmd_bytes[48:50])[0]) + 1
        
        data_size = nlb << self.block_shift
        data_offset = slba << self.block_shift
        
        is_write = (opcode == NVME_CMD_WRITE)
        
        log_nvme.info("NVMe RW: %s SLBA=0x%x NLB=%d Size=%d",
                      'Write' if is_write else 'Read', slba, nlb, data_size)

        if (slba + nlb) > self.ns_size_blocks:
            log_nvme.error("NVMe Error: LBA out of range")
            return NVME_LBA_RANGE | NVME_DNR

        if is_write:
            # DMA Read from Host -> Write to Storage
            data = await self._dma_rw_prp(prp1, prp2, data_size, is_write=True)
            self.storage[data_offset:data_offset+data_size] = data
        else:
            # Read from Storage -> DMA Write to Host
            data = self.storage[data_offset:data_offset+data_size]
            # Ensure we have enough data (pad if necessary, though storage is pre-sized)
            if len(data) < data_size:
                data = data.ljust(data_size, b'\x00')
            await self._dma_rw_prp(prp1, prp2, data_size, is_write=False, data=data)
            
        return NVME_SUCCESS

    async def rpc_nvme_flush(self, cmd_bytes):
        return NVME_SUCCESS

    async def rpc_nvme_write_zeros(self, cmd_bytes):
        slba = struct.unpack("<Q", cmd_bytes[40:48])[0]
        nlb  = (struct.unpack("<H", cmd_bytes[48:50])[0]) + 1
        
        data_size = nlb << self.block_shift
        data_offset = slba << self.block_shift
        
        if (slba + nlb) > self.ns_size_blocks:
            return NVME_LBA_RANGE | NVME_DNR
            
        self.storage[data_offset:data_offset+data_size] = b'\x00' * data_size
        return NVME_SUCCESS

    async def rpc_nvme_io_cmd(self, cmd_bytes):
        dw0 = struct.unpack("<I", cmd_bytes[0:4])[0]
        opcode = dw0 & 0xFF
        nsid = struct.unpack("<I", cmd_bytes[4:8])[0]

        # We assume 1 namespace
        if nsid != 1:
            log_nvme.error("Invalid NSID %d", nsid)
            return NVME_INVALID_NSID | NVME_DNR

        if opcode == NVME_CMD_FLUSH:
            return await self.rpc_nvme_flush(cmd_bytes)
        elif opcode == NVME_CMD_WRITE_ZEROES:
            return await self.rpc_nvme_write_zeros(cmd_bytes)
        elif opcode == NVME_CMD_WRITE or opcode == NVME_CMD_READ:
            return await self.rpc_nvme_rw(cmd_bytes)
        else:
            log_nvme.warning("Invalid IO Opcode %d", opcode)
            return NVME_INVALID_OPCODE | NVME_DNR

    async def rpc_nvme_identify_ctrl(self, prp1):
        data = bytearray(4096)
        if hasattr(self.ctrl, 'id_ctrl'):
            data = bytes(self.ctrl.id_ctrl)
        else:
            struct.pack_into("<H", data, 0, 0x1b36) # VID
            struct.pack_into("<H", data, 2, 0x1b36) # SSVID
            data[4:24] = b"PYTHON_NVME".ljust(20, b' ') # SN
            data[24:64] = b"QEMU NVMe Emulated".ljust(40, b' ') # MN
            data[64:72] = b"1.0".ljust(8, b' ') # FR
            struct.pack_into("<I", data, 516, 1) # NN
        await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, prp1, 4096, data=data, rw=True)
        return NVME_SUCCESS, 0

    async def rpc_nvme_identify_ns(self, prp1, nsid):
        data = bytearray(4096)
        if 1 <= nsid <= len(self.namespaces):
            ns = self.namespaces[nsid - 1]
            id_ns_bytes = bytes(ns.id_ns)
            data[:len(id_ns_bytes)] = id_ns_bytes
            
            log_nvme.info(f"Identify NS {nsid}: NSZE={ns.id_ns.nsze} NCAP={ns.id_ns.ncap} "
                          f"NUSE={ns.id_ns.nuse} LBAF0.DS={ns.id_ns.lbaf[0].ds}")
        await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, prp1, 4096, data=bytes(data), rw=True)
        return NVME_SUCCESS, 0

    async def rpc_nvme_identify_nslist(self, prp1, nsid):
        data = bytearray(4096)
        if nsid < 1:
            struct.pack_into("<I", data, 0, 1)
        await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, prp1, 4096, data=bytes(data), rw=True)
        return NVME_SUCCESS, 0

    async def rpc_nvme_identify(self, cmd_bytes):
        nsid = struct.unpack("<I", cmd_bytes[4:8])[0]
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        cdw10 = struct.unpack("<I", cmd_bytes[40:44])[0]
        cns = cdw10 & 0xFF

        log_nvme.info("Identify command CNS=%d NSID=%d", cns, nsid)

        if cns == 0x00: # Identify Namespace
            if nsid == 0 or nsid > self.ctrl.id_ctrl.nn:
                return NVME_INVALID_NSID | NVME_DNR, 0
            return await self.rpc_nvme_identify_ns(prp1, nsid)
        elif cns == 0x01: # Identify Controller
            return await self.rpc_nvme_identify_ctrl(prp1)
        elif cns == 0x02: # Active Namespace ID list
            return await self.rpc_nvme_identify_nslist(prp1, nsid)
        
        return NVME_INVALID_FIELD | NVME_DNR, 0

    async def rpc_nvme_create_sq(self, cmd_bytes):
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        sqid = struct.unpack("<H", cmd_bytes[40:42])[0]
        qsize = struct.unpack("<H", cmd_bytes[42:44])[0] + 1
        sq_flags = struct.unpack("<H", cmd_bytes[44:46])[0]
        cqid = struct.unpack("<H", cmd_bytes[46:48])[0]
        
        try:
            sq = self.header.make_struct_instance('RPCNvmeSQueue')
        except Exception:
            sq_cls = getattr(self.header, 'RPCNvmeSQueue', None) or self.header.parser.get_struct('RPCNvmeSQueue')
            sq = sq_cls()

        self.rpc_nvme_init_sq(sq, self.ctrl, prp1, sqid, cqid, qsize)
        self.sqs[sqid] = sq

        log_nvme.info("Create SQ: SQID=%d CQID=%d Size=%d PRP1=0x%x", sqid, cqid, qsize, prp1)
        return NVME_SUCCESS, 0

    async def rpc_nvme_del_sq(self, cmd_bytes):
        sqid = struct.unpack("<H", cmd_bytes[40:42])[0]
        self.sqs.pop(sqid, None)
        log_nvme.info("Delete SQ: SQID=%d", sqid)
        return NVME_SUCCESS, 0

    async def rpc_nvme_create_cq(self, cmd_bytes):
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        cqid = struct.unpack("<H", cmd_bytes[40:42])[0]
        qsize = struct.unpack("<H", cmd_bytes[42:44])[0] + 1
        cq_flags = struct.unpack("<H", cmd_bytes[44:46])[0]
        irq_vector = struct.unpack("<H", cmd_bytes[46:48])[0]
        
        try:
            cq = self.header.make_struct_instance('RPCNvmeCQueue')
        except Exception:
            cq_cls = getattr(self.header, 'RPCNvmeCQueue', None) or self.header.parser.get_struct('RPCNvmeCQueue')
            cq = cq_cls()

        irq_enabled = (cq_flags & 0x2) != 0
        self.rpc_nvme_init_cq(cq, self.ctrl, prp1, cqid, irq_vector, qsize, irq_enabled)
        self.cqs[cqid] = cq

        log_nvme.info("Create CQ: CQID=%d Size=%d Vector=%d PRP1=0x%x", cqid, qsize, irq_vector, prp1)
        return NVME_SUCCESS, 0

    async def rpc_nvme_del_cq(self, cmd_bytes):
        cqid = struct.unpack("<H", cmd_bytes[40:42])[0]
        self.cqs.pop(cqid, None)
        log_nvme.info("Delete CQ: CQID=%d", cqid)
        return NVME_SUCCESS, 0

    async def rpc_nvme_get_log_page(self, cmd_bytes):
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        prp2 = struct.unpack("<Q", cmd_bytes[32:40])[0]
        cdw10 = struct.unpack("<I", cmd_bytes[40:44])[0]
        
        lid = cdw10 & 0xFF
        numd = (cdw10 >> 16) & 0xFFF
        num_bytes = (numd + 1) * 4
        
        log_nvme.info("Get Log Page: LID=0x%02x NumBytes=%d", lid, num_bytes)
        
        data = bytearray(num_bytes)
        await self._dma_rw_prp(prp1, prp2, num_bytes, is_write=False, data=bytes(data))
        return NVME_SUCCESS, 0

    async def rpc_nvme_set_feature_timestamp(self, cmd_bytes):
        prp1 = struct.unpack("<Q", cmd_bytes[24:32])[0]
        prp2 = struct.unpack("<Q", cmd_bytes[32:40])[0]
        
        data = await self._dma_rw_prp(prp1, prp2, 8, is_write=True)
        timestamp = struct.unpack("<Q", data)[0]
        self.ctrl.host_timestamp = timestamp
        return NVME_SUCCESS, 0

    async def rpc_nvme_set_feature(self, cmd_bytes):
        dw10 = struct.unpack("<I", cmd_bytes[40:44])[0]
        dw11 = struct.unpack("<I", cmd_bytes[44:48])[0]
        fid = dw10 & 0xFF
        
        result = 0
        
        if fid == NVME_VOLATILE_WRITE_CACHE:
            pass
        elif fid == NVME_NUMBER_OF_QUEUES:
            num_queues = getattr(self.ctrl, 'num_queues', 64)
            val = (num_queues - 2) & 0xFFFF
            result = val | (val << 16)
            log_nvme.info("Set Feature: Num Queues. Result=0x%x", result)
        elif fid == NVME_TIMESTAMP:
            return await self.rpc_nvme_set_feature_timestamp(cmd_bytes)
        else:
            log_nvme.warning("Set Feature: FID=0x%02x not implemented", fid)
            return NVME_INVALID_FIELD | NVME_DNR, 0
            
        return NVME_SUCCESS, result

    async def rpc_nvme_get_feature(self, cmd_bytes):
        fid = struct.unpack("<I", cmd_bytes[40:44])[0] & 0xFF
        log_nvme.info("Get Feature: FID=0x%02x", fid)
        return NVME_SUCCESS, 0

    async def rpc_nvme_abort(self, cmd_bytes):
        cdw10 = struct.unpack("<I", cmd_bytes[40:44])[0]
        sqid = (cdw10 >> 16) & 0xFFFF
        cid = cdw10 & 0xFFFF
        log_nvme.info("Abort: SQID=%d CID=%d", sqid, cid)
        return NVME_SUCCESS, 0

    async def rpc_nvme_abort(self, cmd_bytes):
        cdw10 = struct.unpack("<I", cmd_bytes[40:44])[0]
        sqid = (cdw10 >> 16) & 0xFFFF
        cid = cdw10 & 0xFFFF
        log_nvme.info("Abort: SQID=%d CID=%d", sqid, cid)
        return NVME_SUCCESS, 0

    async def rpc_nvme_admin_cmd(self, cmd_bytes):
        dw0 = struct.unpack("<I", cmd_bytes[0:4])[0]
        opcode = dw0 & 0xFF
        
        log_nvme.debug("opcode:%d", opcode)
        
        if opcode == NVME_ADM_CMD_DELETE_SQ:
            return await self.rpc_nvme_del_sq(cmd_bytes)
        elif opcode == NVME_ADM_CMD_CREATE_SQ:
            return await self.rpc_nvme_create_sq(cmd_bytes)
        elif opcode == NVME_ADM_CMD_GET_LOG_PAGE:
            return await self.rpc_nvme_get_log_page(cmd_bytes)
        elif opcode == NVME_ADM_CMD_DELETE_CQ:
            return await self.rpc_nvme_del_cq(cmd_bytes)
        elif opcode == NVME_ADM_CMD_CREATE_CQ:
            return await self.rpc_nvme_create_cq(cmd_bytes)
        elif opcode == NVME_ADM_CMD_IDENTIFY:
            return await self.rpc_nvme_identify(cmd_bytes)
        elif opcode == NVME_ADM_CMD_ABORT:
            return await self.rpc_nvme_abort(cmd_bytes)
        elif opcode == NVME_ADM_CMD_ABORT:
            return await self.rpc_nvme_abort(cmd_bytes)
        elif opcode == NVME_ADM_CMD_SET_FEATURES:
            return await self.rpc_nvme_set_feature(cmd_bytes)
        elif opcode == NVME_ADM_CMD_GET_FEATURES:
            return await self.rpc_nvme_get_feature(cmd_bytes)

        log_nvme.warning("Admin Opcode 0x%02x not implemented", opcode)
        return NVME_INVALID_OPCODE | NVME_DNR, 0

    async def _process_cmd(self, sq, cq):
        # Fetch SQE
        sqe_addr = sq.dma_addr + (sq.head * 64)
        ret, sqe_data = await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, sqe_addr, 64)
        
        if ret != 0 or len(sqe_data) != 64:
            log_nvme.error(f"Failed to read SQE for QID {sq.sqid} at 0x{sqe_addr:x}")
            sq.head = sq.tail # Stop processing
            return

        # Update SQ Head
        sq.head = (sq.head + 1) % sq.size

        # Decode SQE
        dw0 = struct.unpack("<I", sqe_data[0:4])[0]
        cid = (dw0 >> 16) & 0xFFFF

        # Call appropriate handler
        if sq.sqid == 0:
            status, result = await self.rpc_nvme_admin_cmd(sqe_data)
        else:
            status = await self.rpc_nvme_io_cmd(sqe_data)
            result = 0

        # Post CQE
        cqe = bytearray(16)
        struct.pack_into("<I", cqe, 0, result)
        struct.pack_into("<H", cqe, 8, sq.head)
        struct.pack_into("<H", cqe, 12, cid)
        status_field = (status << 1) | cq.phase
        struct.pack_into("<H", cqe, 14, status_field)
        
        cqe_addr = cq.dma_addr + (cq.head * 16)
        await self.remote_port_ctrl.rp_mm_access(RPDEV_PCI_DMA, cqe_addr, 16, data=bytes(cqe), rw=True)
        
        # Update CQ Head and toggle Phase Tag if wrapped
        cq.head = (cq.head + 1) % cq.size
        if cq.head == 0:
            cq.phase = 1 - cq.phase

        await self.send_nvme_irq(cq)
        
        log_nvme.debug(f"Post CQE for QID {cq.cqid}, head:{cq.head}, status:{status}, result:{result}")
        
    async def _process_sq(self, qid):
        try:
            if qid == 0:
                sq = self.ctrl.admin_sq
                cq = self.ctrl.admin_cq
            else:
                sq = self.sqs.get(qid)
                if not sq:
                    log_nvme.error(f"Worker for QID {qid} started with invalid SQ")
                    return
                cq = self.cqs.get(sq.cqid)
                if not cq:
                    log_nvme.error(f"Worker for QID {qid} started with invalid CQ (cqid={sq.cqid})")
                    return
            
            while sq.head != sq.tail:
                await self._process_cmd(sq, cq)
            
            log_nvme.debug(f"_process_sq for QID {qid} finished")
        except Exception as e:
            log_main.error(f"Exception in SQ worker for QID {qid}: {e}", exc_info=True)

    async def run(self):
        log_main.info("Starting run loop...")
        while True:
            try:
                hdr = await self.remote_port_ctrl.reader.readexactly(20)

                cmd, length, pid, flags, dev = struct.unpack(">IIIII", hdr)
                payload = await self.remote_port_ctrl.reader.readexactly(length) if length else b""
                pkt_header = (cmd, length, pid, flags, dev)

                if flags & RP_PKT_FLAGS_response:
                    self.remote_port_ctrl.dispatch_response(pid, cmd, length, flags, dev, payload)
                    continue

                log_msg = f"RECV cmd={cmd} dev={dev} len={length} pid={pid}"
                if cmd == RP_CMD_sync:
                    log_sync.info(log_msg)
                elif cmd in [RP_CMD_read, RP_CMD_write]:
                    log_bus.info(log_msg)
                elif dev in [RPDEV_PCI_BAR_BASE, RPDEV_NVME_CTRL, RPDEV_PCI_CONFIG]:
                    log_nvme.info(log_msg)
                else:
                    log_main.info(log_msg)

                # HELLO / SYNC / INTERRUPT are not bus access commands
                if cmd == RP_CMD_hello:
                    await self.send_pkt(cmd, 0, payload, RP_PKT_FLAGS_response, pid)
                    continue
                if cmd == RP_CMD_sync:
                    await self.send_pkt(cmd, 0, payload, RP_PKT_FLAGS_response, pid)
                    continue
                if cmd == RP_CMD_interrupt:
                    await self.send_pkt(cmd, dev, payload, RP_PKT_FLAGS_response, pid)
                    continue

                # --- Bus Access Command Handling ---

                # Handle NVMe MMIO write using the new translated function
                if cmd == RP_CMD_write and dev in [RPDEV_PCI_BAR_BASE, RPDEV_NVME_CTRL]:
                    response_info, worker_to_start, cq_to_deassert = self.rpc_nvme_rp_io_write(pkt_header, payload)
                    if response_info:
                        resp_payload, resp_flags = response_info
                        await self.send_pkt(cmd, dev, resp_payload, resp_flags, pid)

                    if cq_to_deassert:
                        asyncio.create_task(self.rpc_nvme_irq_deassert(cq_to_deassert))

                    if worker_to_start:
                        worker_type, qid = worker_to_start
                        if worker_type == 'admin':
                            worker_key = 'admin'
                            log_msg_start = "Starting admin SQ worker"
                        else: # 'io'
                            worker_key = f'io_{qid}'
                            log_msg_start = f"Starting IO SQ worker for QID {qid}"

                        task = self.worker.get(worker_key)
                        if task is None or task.done():
                            log_nvme.info(log_msg_start)
                            self.worker[worker_key] = asyncio.create_task(self._process_sq(qid))
                        else:
                            log_nvme.debug(f"Worker for QID {qid} already running")
                    continue

                # Fallback to old logic for other bus access commands (reads, PCI config write)
                ba = self.remote_port_ctrl.parse_bus_access(payload)
                if ba is None:
                    await self.send_pkt(cmd, dev, b"", RP_PKT_FLAGS_response, pid)
                    continue

                addr, ln, data, hdrb = ba

                # Ensure width is 0 in response
                resp = bytearray(hdrb)
                struct.pack_into(">I", resp, 28, 0)

                if dev == RPDEV_PCI_CONFIG and cmd == RP_CMD_read:
                    read_data = self.pci_config[addr:addr + ln]
                    log_nvme.debug("PCI Cfg Read: addr=0x%x, len=%d, data=%s", addr, ln, read_data.hex())
                    resp += read_data
                elif dev == RPDEV_PCI_CONFIG and cmd == RP_CMD_write:
                    log_nvme.debug("PCI Cfg Write: addr=0x%x, len=%d, data=%s", addr, len(data), data.hex())
                    self.pci_config[addr:addr + len(data)] = data
                elif dev in [RPDEV_PCI_BAR_BASE, RPDEV_NVME_CTRL] and cmd == RP_CMD_read:
                    resp += self._bar0_read(addr, ln)

                # If we reach here, it's a command that needs a default response.
                await self.send_pkt(cmd, dev, resp, RP_PKT_FLAGS_response, pid)
            except (asyncio.IncompleteReadError, ConnectionError):
                log_main.info("Connection closed by remote.")
                break
            except Exception as e:
                log_main.error("Exception in run loop: %s", e, exc_info=True)
                break

    async def start(self):
        # Advertise RP_CAP_BUSACCESS_EXT_BASE (bit 2) to ensure 54-byte header
        caps = [1 << 2]
        await self.remote_port_ctrl.rp_hello(caps=caps)
        await self.run()
# ============================================================
# Entry
# ============================================================

if __name__ == "__main__":
    client = RnvmeClient()
    async def main():
        await client.connect(sys.argv[1] if len(sys.argv) > 1 else "build/pl-rp")
        await client.start()
    asyncio.run(main())
