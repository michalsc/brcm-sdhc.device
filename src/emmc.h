#ifndef _EMMC_H
#define _EMMC_H

#include "sdcard.h"

ULONG getclock(struct SDCardBase *SDCardBase);
int powerCycle(struct SDCardBase *SDCardBase);
void led(int on, struct SDCardBase *SDCardBase);
void cmd_int(ULONG cmd, ULONG arg, ULONG timeout, struct SDCardBase *SDCardBase);
void cmd(ULONG command, ULONG arg, ULONG timeout, struct SDCardBase *SDCardBase);
int sd_card_init(struct SDCardBase *SDCardBase);
int sd_write(uint8_t *buf, uint32_t buf_size, uint32_t block_no, struct SDCardBase *SDCardBase);
int sd_read(uint8_t *buf, uint32_t buf_size, uint32_t block_no, struct SDCardBase *SDCardBase);

#endif /* _EMMC_H */