#ifndef HW_NVME_H
#define HW_NVME_H
#include "block/nvme.h"

typedef struct rps_nvme_misc_info{
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
}rps_nvme_misc_info_t;

typedef struct RPSNvmeAsyncEvent {
    QSIMPLEQ_ENTRY(RPSNvmeAsyncEvent) entry;
    NvmeAerResult result;
} RPSNvmeAsyncEvent;

typedef struct RPSNvmeRequest {
    struct RPSNvmeSQueue       *sq;
    BlockAIOCB              *aiocb;
    uint16_t                status;
    bool                    has_sg;
    NvmeCqe                 cqe;
    BlockAcctCookie         acct;
    QEMUSGList              qsg;
    QEMUIOVector            iov;
    QTAILQ_ENTRY(RPSNvmeRequest)entry;
} RPSNvmeRequest;

typedef struct RPSNvmeSQueue {
    struct RPSNvmeCtrl *ctrl;
    uint16_t    sqid;
    uint16_t    cqid;
    uint32_t    head;
    uint32_t    tail;
    uint32_t    size;
    uint64_t    dma_addr;
    QEMUTimer   *timer;
    RPSNvmeRequest *io_req;
    MemoryRegion    iomem;
    QTAILQ_HEAD(, RPSNvmeRequest) req_list;
    QTAILQ_HEAD(, RPSNvmeRequest) out_req_list;
    QTAILQ_ENTRY(RPSNvmeSQueue) entry;
} RPSNvmeSQueue;

typedef struct RPSNvmeCQueue {
    struct RPSNvmeCtrl *ctrl;
    uint8_t     phase;
    uint16_t    cqid;
    uint16_t    irq_enabled;
    uint32_t    head;
    uint32_t    tail;
    uint32_t    vector;
    uint32_t    size;
    uint64_t    dma_addr;
    QEMUTimer   *timer;
    QTAILQ_HEAD(, RPSNvmeSQueue) sq_list;
    QTAILQ_HEAD(, RPSNvmeRequest) req_list;
} RPSNvmeCQueue;

typedef struct RPSNvmeNamespace {
    NvmeIdNs        id_ns;
} RPSNvmeNamespace;

#define TYPE_RPSNVME "rpsnvme"
#define RPSNVME(obj) \
        OBJECT_CHECK(RPSNvmeCtrl, (obj), TYPE_RPSNVME)

typedef struct RPSNvmeCtrl {
    PCIDevice    parent_obj;
    MemoryRegion iomem;
    MemoryRegion ctrl_mem;
    NvmeBar      bar;

    RemotePortMemorySlave *rp_dma;
	RemotePortATS *rp_ats;
    BlockConf    conf;
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

    RPSNvmeNamespace   *namespaces;
    RPSNvmeSQueue      **sq;
    RPSNvmeCQueue      **cq;
    RPSNvmeSQueue      admin_sq;
    RPSNvmeCQueue      admin_cq;
    NvmeIdCtrl      id_ctrl;

    struct RemotePort *rp;
	struct rp_peer_state *peer;
	RemotePortMap *maps;
} RPSNvmeCtrl;

#endif /* HW_NVME_H */
