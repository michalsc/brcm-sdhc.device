#ifndef _MBOX_H
#define _MBOX_H

#include "sdcard.h"
#include <stdint.h>

uint32_t get_clock_rate(uint32_t clock_id, struct SDCardBase * SDCardBase);
uint32_t set_clock_rate(uint32_t clock_id, uint32_t speed, struct SDCardBase * SDCardBase);
uint32_t get_clock_state(uint32_t id, struct SDCardBase * SDCardBase);
uint32_t set_clock_state(uint32_t id, uint32_t state, struct SDCardBase * SDCardBase);
uint32_t get_power_state(uint32_t id, struct SDCardBase * SDCardBase);
uint32_t set_power_state(uint32_t id, uint32_t state, struct SDCardBase * SDCardBase);
uint32_t set_led_state(uint32_t id, uint32_t state, struct SDCardBase * SDCardBase);
uint32_t get_extgpio_state(uint32_t id, struct SDCardBase * SDCardBase);
uint32_t get_extgpio_state(uint32_t id, struct SDCardBase * SDCardBase);
uint32_t set_extgpio_state(uint32_t id, uint32_t state, struct SDCardBase * SDCardBase);
uint32_t set_sdhost_clock(uint32_t speed, struct SDCardBase * SDCardBase);

#endif /* _MBOX_H */
