#ifndef HW_NVME_H
#define HW_NVME_H
#include "block/nvme.h"

typedef struct rpc_nvme_misc_info{
    uint32_t    page_size;
    uint16_t    page_bits;
    uint16_t    max_prp_ents;
    uint16_t    cqe_size;
    uint16_t    sqe_size;
    uint32_t    reg_size;
    uint32_t    num_namespaces;
    uint32_t    num_queues;
    uint32_t    max_q_ents;
    uint64_t    ns_size;
    uint64_t    bs_size;
    uint32_t    cmb_size_mb;
    uint32_t    cmbsz;
    uint32_t    cmbloc;
}rpc_nvme_misc_info_t;

typedef struct RPCNvmeAsyncEvent {
    QSIMPLEQ_ENTRY(RPCNvmeAsyncEvent) entry;
    NvmeAerResult result;
} RPCNvmeAsyncEvent;

typedef struct RPCNvmeRequest {
    struct RPCNvmeSQueue       *sq;
    BlockAIOCB              *aiocb;
    uint16_t                status;
    bool                    has_sg;
    NvmeCqe                 cqe;
    BlockAcctCookie         acct;
    QEMUSGList              qsg;
    QEMUIOVector            iov;
    QTAILQ_ENTRY(RPCNvmeRequest)entry;
} RPCNvmeRequest;

typedef struct RPCNvmeSQueue {
    struct RPCNvmeCtrl *ctrl;
    uint16_t    sqid;
    uint16_t    cqid;
    uint32_t    head;
    uint32_t    tail;
    uint32_t    size;
    uint64_t    dma_addr;
    QEMUTimer   *timer;
    RPCNvmeRequest *io_req;
    QTAILQ_HEAD(, RPCNvmeRequest) req_list;
    QTAILQ_HEAD(, RPCNvmeRequest) out_req_list;
    QTAILQ_ENTRY(RPCNvmeSQueue) entry;
} RPCNvmeSQueue;

typedef struct RPCNvmeCQueue {
    struct RPCNvmeCtrl *ctrl;
    uint8_t     phase;
    uint16_t    cqid;
    uint16_t    irq_enabled;
    uint32_t    head;
    uint32_t    tail;
    uint32_t    vector;
    uint32_t    size;
    uint64_t    dma_addr;
    QEMUTimer   *timer;
    QTAILQ_HEAD(, RPCNvmeSQueue) sq_list;
    QTAILQ_HEAD(, RPCNvmeRequest) req_list;
} RPCNvmeCQueue;

typedef struct RPCNvmeNamespace {
    NvmeIdNs        id_ns;
} RPCNvmeNamespace;

#define TYPE_RPCNVME "rpcnvme"
#define RPCNVME(obj) \
        OBJECT_CHECK(RPCNvmeCtrl, (obj), TYPE_RPCNVME)

typedef struct RPCNvmeCtrl {
    PCIDevice    parent_obj;
    MemoryRegion iomem;
    MemoryRegion ctrl_mem;
    NvmeBar      bar;
    BlockConf    conf;
    RemotePortMemorySlave *rp_dma;
   	RemotePortATS *rp_ats;
    struct {
           uint32_t rp_dev;
           uint32_t nr_io_bars;
           uint32_t nr_mm_bars;
           uint64_t bar_size[6];
           uint32_t nr_devs;
           uint32_t vendor_id;
           uint32_t device_id;
           uint32_t revision;
           uint32_t class_id;
           uint8_t prog_if;
           uint8_t irq_pin;

           /* Controls if the remote dev is responsible for the config space.  */
           bool remote_config;

           bool msi;
           bool msix;
           bool ats;
    } cfg;
    uint32_t    page_size;
    uint16_t    page_bits;
    uint16_t    max_prp_ents;
    uint16_t    cqe_size;
    uint16_t    sqe_size;
    uint32_t    reg_size;
    uint32_t    num_namespaces;
    uint32_t    num_queues;
    uint32_t    max_q_ents;
    uint64_t    ns_size;
    uint32_t    cmb_size_mb;
    uint32_t    cmbsz;
    uint32_t    cmbloc;
    uint8_t     *cmbuf;
    uint64_t    irq_status;
    uint64_t    host_timestamp;                 /* Timestamp sent by the host */
    uint64_t    timestamp_set_qemu_clock_ms;    /* QEMU clock time */

    char            *serial;
    HostMemoryBackend *pmrdev;

    RPCNvmeNamespace   *namespaces;
    RPCNvmeSQueue      **sq;
    RPCNvmeCQueue      **cq;
    RPCNvmeSQueue      admin_sq;
    RPCNvmeCQueue      admin_cq;
    NvmeIdCtrl      id_ctrl;
    struct RemotePort *rp;
    struct rp_peer_state *peer;
} RPCNvmeCtrl;

#endif /* HW_NVME_H */
