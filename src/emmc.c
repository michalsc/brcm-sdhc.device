/*
    Copyright © 2021 Michal Schulz <michal.schulz@gmx.de>
    https://github.com/michalsc

    This Source Code Form is subject to the terms of the
    Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed
    with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/


#include <exec/types.h>
#include <exec/execbase.h>
#include <exec/io.h>
#include <exec/errors.h>

#include <proto/exec.h>
#include <proto/expansion.h>
#include <proto/devicetree.h>

#include <libraries/configregs.h>
#include <libraries/configvars.h>

#include <stdint.h>
#include "sdcard.h"
#include "emmc.h"

ULONG getclock(struct SDCardBase *SDCardBase)
{
    ULONG clock = SDCardBase->get_clock_rate(1, SDCardBase);
    if (clock == 0)
        clock = 50000000;
    return clock;
}

void led(int on, struct SDCardBase *SDCardBase)
{
    if (on) {
        wr32((APTR)0xf2200000, 0x1c, 1 << 29);
    }
    else {
        wr32((APTR)0xf2200000, 0x28, 1 << 29);
    }
}

int powerCycle(struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

    RawDoFmt("[brcm-sdhc] powerCycle\n", NULL, (APTR)putch, NULL);

    RawDoFmt("[brcm-sdhc]   power OFF\n", NULL, (APTR)putch, NULL);
    SDCardBase->set_power_state(0, 2, SDCardBase);

    SDCardBase->sd_Delay(500000, SDCardBase);

    RawDoFmt("[brcm-sdhc]   power ON\n", NULL, (APTR)putch, NULL);
    return SDCardBase->set_power_state(0, 3, SDCardBase);
}

void cmd_int(ULONG cmd, ULONG arg, ULONG timeout, struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

    ULONG tout = 0;
    SDCardBase->sd_LastCMDSuccess = 0;

    // Check Command Inhibit
    while(rd32(SDCardBase->sd_SDHC, EMMC_STATUS) & 0x1)
        SDCardBase->sd_Delay(10, SDCardBase);

    // Is the command with busy?
    if((cmd & SD_CMD_RSPNS_TYPE_MASK) == SD_CMD_RSPNS_TYPE_48B)
    {
        // With busy

        // Is is an abort command?
        if((cmd & SD_CMD_TYPE_MASK) != SD_CMD_TYPE_ABORT)
        {
            // Not an abort command

            // Wait for the data line to be free
            while(rd32(SDCardBase->sd_SDHC, EMMC_STATUS) & 0x2)
                SDCardBase->sd_Delay(10, SDCardBase);
        }
    }

    uint32_t blksizecnt = SDCardBase->sd_BlockSize | (SDCardBase->sd_BlocksToTransfer << 16);

    wr32(SDCardBase->sd_SDHC, EMMC_BLKSIZECNT, blksizecnt);

    // Set argument 1 reg
    wr32(SDCardBase->sd_SDHC, EMMC_ARG1, arg);

#if 0
    {
        ULONG args[] = {cmd, arg};
        RawDoFmt("[brcm-sdhc] sending command %08lx, arg %08lx\n", args, (APTR)putch, NULL);
    }
#endif

    // Set command reg
    wr32(SDCardBase->sd_SDHC, EMMC_CMDTM, cmd);

asm volatile("nop");
    //SDCardBase->sd_Delay(10, SDCardBase);

    // Wait for command complete interrupt
    TIMEOUT_WAIT((rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT) & 0x8001), timeout);
    uint32_t irpts = rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT);

    // Clear command complete status
    wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffff0001);

    // Test for errors
    if((irpts & 0xffff0001) != 0x1)
    {
        RawDoFmt("[brcm-sdhc] error occured whilst waiting for command complete interrupt (%08lx)\n", &irpts, (APTR)putch, NULL);

        SDCardBase->sd_LastError = irpts & 0xffff0000;
        SDCardBase->sd_LastInterrupt = irpts;
        return;
    }

//    SDCardBase->sd_Delay(10, SDCardBase);
    asm volatile("nop");

    // Get response data
    switch(cmd & SD_CMD_RSPNS_TYPE_MASK)
    {
        case SD_CMD_RSPNS_TYPE_48:
        case SD_CMD_RSPNS_TYPE_48B:
            SDCardBase->sd_Res0 = rd32(SDCardBase->sd_SDHC, EMMC_RESP0);
            break;

        case SD_CMD_RSPNS_TYPE_136:
            SDCardBase->sd_Res0 = rd32(SDCardBase->sd_SDHC, EMMC_RESP0);
            SDCardBase->sd_Res1 = rd32(SDCardBase->sd_SDHC, EMMC_RESP1);
            SDCardBase->sd_Res2 = rd32(SDCardBase->sd_SDHC, EMMC_RESP2);
            SDCardBase->sd_Res3 = rd32(SDCardBase->sd_SDHC, EMMC_RESP3);
            break;
    }
    // If with data, wait for the appropriate interrupt
    if(cmd & SD_CMD_ISDATA)
    {
        uint32_t wr_irpt;
        int is_write = 0;
        if(cmd & SD_CMD_DAT_DIR_CH)
            wr_irpt = (1 << 5);     // read
        else
        {
            is_write = 1;
            wr_irpt = (1 << 4);     // write
        }

        int cur_block = 0;
        uint32_t *cur_buf_addr = (uint32_t *)SDCardBase->sd_Buffer;
        while(cur_block < SDCardBase->sd_BlocksToTransfer)
        {
            tout = timeout / 100;
            TIMEOUT_WAIT((rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT) & (wr_irpt | 0x8000)), timeout);
            irpts = rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT);
            wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffff0000 | wr_irpt);

            if((irpts & (0xffff0000 | wr_irpt)) != wr_irpt)
            {
                RawDoFmt("[brcm-sdhc] error occured whilst waiting for data ready interrupt (%08lx)\n", &irpts, (APTR)putch, NULL);

                SDCardBase->sd_LastError = irpts & 0xffff0000;
                SDCardBase->sd_LastInterrupt = irpts;
                return;
            }

            // Transfer the block
            UWORD cur_byte_no = 0;
            while(cur_byte_no < SDCardBase->sd_BlockSize)
            {
                if(is_write)
				{
					uint32_t data = *(ULONG*)cur_buf_addr;
                    wr32be(SDCardBase->sd_SDHC, EMMC_DATA, data);
				}
                else
				{
					uint32_t data = rd32be(SDCardBase->sd_SDHC, EMMC_DATA);
					*(ULONG*)cur_buf_addr = data;
				}
                cur_byte_no += 4;
                cur_buf_addr++;
            }

            cur_block++;
        }
    }
    // Wait for transfer complete (set if read/write transfer or with busy)
    if((((cmd & SD_CMD_RSPNS_TYPE_MASK) == SD_CMD_RSPNS_TYPE_48B) ||
       (cmd & SD_CMD_ISDATA)))
    {
        // First check command inhibit (DAT) is not already 0
        if((rd32(SDCardBase->sd_SDHC, EMMC_STATUS) & 0x2) == 0)
            wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffff0002);
        else
        {
            TIMEOUT_WAIT((rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT) & 0x8002), timeout);
            irpts = rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT);
            wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffff0002);

            // Handle the case where both data timeout and transfer complete
            //  are set - transfer complete overrides data timeout: HCSS 2.2.17
            if(((irpts & 0xffff0002) != 0x2) && ((irpts & 0xffff0002) != 0x100002))
            {
                RawDoFmt("[brcm-sdhc] error occured whilst waiting for transfer complete interrupt (%08lx)\n", &irpts, (APTR)putch, NULL);
                SDCardBase->sd_LastError = irpts & 0xffff0000;
                SDCardBase->sd_LastInterrupt = irpts;
                return;
            }
            wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffff0002);
        }
    }
    SDCardBase->sd_LastCMDSuccess = 1;
}

// Reset the CMD line
static int sd_reset_cmd(struct SDCardBase *SDCardBase)
{
    int tout = 1000000;
    uint32_t control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
	control1 |= SD_RESET_CMD;
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
	while (tout && (rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & SD_RESET_CMD) != 0) {
        SDCardBase->sd_Delay(1, SDCardBase);
        tout--;
    }
	if((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & SD_RESET_CMD) != 0)
	{
		return -1;
	}
	return 0;
}

// Reset the CMD line
static int sd_reset_dat(struct SDCardBase *SDCardBase)
{
    int tout = 1000000;
    uint32_t control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
	control1 |= SD_RESET_DAT;
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
	while (tout && (rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & SD_RESET_DAT) != 0) {
        SDCardBase->sd_Delay(1, SDCardBase);
        tout--;
    }
	if((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & SD_RESET_DAT) != 0)
	{
		return -1;
	}
	return 0;
}

static void sd_handle_card_interrupt(struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

    // Handle a card interrupt

    // Get the card status
    if(SDCardBase->sd_CardRCA)
    {
        cmd_int(SEND_STATUS, SDCardBase->sd_CardRCA << 16, 500000, SDCardBase);
        if(FAIL(SDCardBase))
        {
        }
        else
        {
        }
    }
    else
    {
    }
}

static void sd_handle_interrupts(struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;
    uint32_t irpts = rd32(SDCardBase->sd_SDHC, EMMC_INTERRUPT);
    uint32_t reset_mask = 0;

    if(irpts & SD_COMMAND_COMPLETE)
    {
        RawDoFmt("[brcm-sdhc] spurious command complete interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_COMMAND_COMPLETE;
    }

    if(irpts & SD_TRANSFER_COMPLETE)
    {
        RawDoFmt("[brcm-sdhc] spurious transfer complete interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_TRANSFER_COMPLETE;
    }

    if(irpts & SD_BLOCK_GAP_EVENT)
    {
        RawDoFmt("[brcm-sdhc] spurious block gap event interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_BLOCK_GAP_EVENT;
    }

    if(irpts & SD_DMA_INTERRUPT)
    {
        RawDoFmt("[brcm-sdhc] spurious DMA interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_DMA_INTERRUPT;
    }

    if(irpts & SD_BUFFER_WRITE_READY)
    {
        RawDoFmt("[brcm-sdhc] spurious buffer write ready interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_BUFFER_WRITE_READY;
        sd_reset_dat(SDCardBase);
    }

    if(irpts & SD_BUFFER_READ_READY)
    {
        RawDoFmt("[brcm-sdhc] spurious buffer read ready interrupt\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_BUFFER_READ_READY;
        sd_reset_dat(SDCardBase);
    }

    if(irpts & SD_CARD_INSERTION)
    {
        RawDoFmt("[brcm-sdhc] card insertion detected\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_CARD_INSERTION;
    }

    if(irpts & SD_CARD_REMOVAL)
    {
        RawDoFmt("[brcm-sdhc] card removal detected\n", NULL, (APTR)putch, NULL);
        reset_mask |= SD_CARD_REMOVAL;
        SDCardBase->sd_CardRemoval = 1;
    }

    if(irpts & SD_CARD_INTERRUPT)
    {
        sd_handle_card_interrupt(SDCardBase);
        reset_mask |= SD_CARD_INTERRUPT;
    }

    if(irpts & 0x8000)
    {
        reset_mask |= 0xffff0000;
    }

    wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, reset_mask);
}

void cmd(ULONG command, ULONG arg, ULONG timeout, struct SDCardBase *SDCardBase)
{
    // First, handle any pending interrupts
    sd_handle_interrupts(SDCardBase);

    // Stop the command issue if it was the card remove interrupt that was
    //  handled
    if(SDCardBase->sd_CardRemoval)
    {
        SDCardBase->sd_LastCMDSuccess = 0;
        return;
    }

    // Now run the appropriate commands by calling sd_issue_command_int()
    if(command & IS_APP_CMD)
    {
        command &= 0x7fffffff;

        SDCardBase->sd_LastCMD = APP_CMD;

        uint32_t rca = 0;
        if(SDCardBase->sd_CardRCA)
            rca = SDCardBase->sd_CardRCA << 16;
        cmd_int(APP_CMD, rca, timeout, SDCardBase);
        if(SDCardBase->sd_LastCMDSuccess)
        {
            SDCardBase->sd_LastCMD = command | IS_APP_CMD;
            cmd_int(command, arg, timeout, SDCardBase);
        }
    }
    else
    {
        SDCardBase->sd_LastCMD = command;
        cmd_int(command, arg, timeout, SDCardBase);
    }
}


// Set the clock dividers to generate a target value
static uint32_t sd_get_clock_divider(uint32_t base_clock, uint32_t target_rate)
{
    // TODO: implement use of preset value registers

    uint32_t targetted_divisor = 0;
    if(target_rate > base_clock)
        targetted_divisor = 1;
    else
    {
        targetted_divisor = base_clock / target_rate;
        uint32_t mod = base_clock % target_rate;
        if(mod)
            targetted_divisor--;
    }

    // Decide on the clock mode to use

    // Currently only 10-bit divided clock mode is supported

    // HCI version 3 or greater supports 10-bit divided clock mode
    // This requires a power-of-two divider

    // Find the first bit set
    int divisor = -1;
    for(int first_bit = 31; first_bit >= 0; first_bit--)
    {
        uint32_t bit_test = (1 << first_bit);
        if(targetted_divisor & bit_test)
        {
            divisor = first_bit;
            targetted_divisor &= ~bit_test;
            if(targetted_divisor)
            {
                // The divisor is not a power-of-two, increase it
                divisor++;
            }
            break;
        }
    }

    if(divisor == -1)
        divisor = 31;
    if(divisor >= 32)
        divisor = 31;

    if(divisor != 0)
        divisor = (1 << (divisor - 1));

    if(divisor >= 0x400)
        divisor = 0x3ff;

    uint32_t freq_select = divisor & 0xff;
    uint32_t upper_bits = (divisor >> 8) & 0x3;
    uint32_t ret = (freq_select << 8) | (upper_bits << 6) | (0 << 5);

    return ret;
}

// Switch the clock rate whilst running
static int sd_switch_clock_rate(uint32_t base_clock, uint32_t target_rate, struct SDCardBase *SDCardBase)
{
    // Decide on an appropriate divider
    uint32_t divider = sd_get_clock_divider(base_clock, target_rate);

    // Wait for the command inhibit (CMD and DAT) bits to clear
    while(rd32(SDCardBase->sd_SDHC, EMMC_STATUS) & 0x3)
        SDCardBase->sd_Delay(1000, SDCardBase);

    // Set the SD clock off
    uint32_t control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
    control1 &= ~(1 << 2);
    wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
    SDCardBase->sd_Delay(2000, SDCardBase);

    // Write the new divider
	control1 &= ~0xffe0;		// Clear old setting + clock generator select
    control1 |= divider;
    wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
    SDCardBase->sd_Delay(2000, SDCardBase);

    // Enable the SD clock
    control1 |= (1 << 2);
    wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
    SDCardBase->sd_Delay(2000, SDCardBase);

    return 0;
}

int sd_card_init(struct SDCardBase *SDCardBase)
{
    ULONG tout;
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

    RawDoFmt("[brcm-sdhc] SD Card init\n", NULL, (APTR)putch, NULL);

    SDCardBase->sd_PowerCycle(SDCardBase);

    uint32_t ver = rd32(SDCardBase->sd_SDHC, EMMC_SLOTISR_VER);
	uint32_t vendor = ver >> 24;
	uint32_t sdversion = (ver >> 16) & 0xff;
	uint32_t slot_status = ver & 0xff;
	
    UWORD args[] = { vendor, sdversion, slot_status };
    RawDoFmt("[brcm-sdhc] EMMC: vendor %x, sdversion %x, slot_status %x\n", args, (APTR)putch, NULL);

    uint32_t control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
	control1 |= (1 << 24);
	// Disable clock
	control1 &= ~(1 << 2);
	control1 &= ~(1 << 0);
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
    TIMEOUT_WAIT((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & (0x7 << 24)) == 0, 1000000);
	if((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & (7 << 24)) != 0)
	{
		RawDoFmt("[brcm-sdhc] EMMC: controller did not reset properly\n", NULL, (APTR)putch, NULL);
		return -1;
	}

    {
        ULONG args[] = {
            rd32(SDCardBase->sd_SDHC, EMMC_CONTROL0),
            rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1),
            rd32(SDCardBase->sd_SDHC, EMMC_CONTROL2),
        };
        RawDoFmt("[brcm-sdhc] EMMC: control0: %08lx, control1: %08lx, control2: %08lx\n", args, (APTR)putch, NULL);
    }

    // Read the capabilities registers - NOTE - these are not existing in case of Arasan SDHC from RasPi!!!
	SDCardBase->sd_Capabilities0 = rd32(SDCardBase->sd_SDHC, EMMC_CAPABILITIES_0);
	SDCardBase->sd_Capabilities1 = rd32(SDCardBase->sd_SDHC, EMMC_CAPABILITIES_1);

    {
        ULONG args[] = { SDCardBase->sd_Capabilities0, SDCardBase->sd_Capabilities1};
        RawDoFmt("[brcm-sdhc] Cap0: %08lx, Cap1: %08lx\n", args, (APTR)putch, NULL);
    }

    TIMEOUT_WAIT(rd32(SDCardBase->sd_SDHC, EMMC_STATUS) & (1 << 16), 500000);
	uint32_t status_reg = rd32(SDCardBase->sd_SDHC, EMMC_STATUS);
	if((status_reg & (1 << 16)) == 0)
	{
		RawDoFmt("[brcm-sdhc] EMMC: no card inserted\n", NULL, (APTR)putch, NULL);
		return -1;
	}

	RawDoFmt("[brcm-sdhc] EMMC: status: %08lx\n", &status_reg, (APTR)putch, NULL);

	// Clear control2
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL2, 0);

	// Get the base clock rate
	uint32_t base_clock = SDCardBase->sd_GetBaseClock(SDCardBase);

    RawDoFmt("[brcm-sdhc] Base clock: %ld Hz\n", &base_clock, (APTR)putch, NULL);

	control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
	control1 |= 1;			// enable clock

	// Set to identification frequency (400 kHz)
	uint32_t f_id = sd_get_clock_divider(base_clock, SD_CLOCK_ID);

	control1 |= f_id;

	control1 |= (7 << 16);		// data timeout = TMCLK * 2^10
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
    TIMEOUT_WAIT((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & 0x2), 1000000);
	if((rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1) & 0x2) == 0)
	{
		RawDoFmt("[brcm-sdhc] EMMC: controller's clock did not stabilise within 1 second\n", NULL, (APTR)putch, NULL);
		return -1;
	}

    {
        ULONG args[] = {
            rd32(SDCardBase->sd_SDHC, EMMC_CONTROL0),
            rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1)
        };
        RawDoFmt("[brcm-sdhc] EMMC: control0: %08lx, control1: %08lx\n", args, (APTR)putch, NULL);
    }

	// Enable the SD clock
    SDCardBase->sd_Delay(2000, SDCardBase);
	control1 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL1);
	control1 |= 4;
	wr32(SDCardBase->sd_SDHC, EMMC_CONTROL1, control1);
	SDCardBase->sd_Delay(2000, SDCardBase);

	// Mask off sending interrupts to the ARM
	wr32(SDCardBase->sd_SDHC, EMMC_IRPT_EN, 0);
	// Reset interrupts
	wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffffffff);
	// Have all interrupts sent to the INTERRUPT register
	uint32_t irpt_mask = 0xffffffff & (~SD_CARD_INTERRUPT);
#ifdef SD_CARD_INTERRUPTS
    irpt_mask |= SD_CARD_INTERRUPT;
#endif
	wr32(SDCardBase->sd_SDHC, EMMC_IRPT_MASK, irpt_mask);

	SDCardBase->sd_Delay(2000, SDCardBase);

	// Send CMD0 to the card (reset to idle state)
	SDCardBase->sd_CMD(GO_IDLE_STATE, 0, 500000, SDCardBase);
	if(FAIL(SDCardBase))
	{
        RawDoFmt("[brcm-sdhc] SD: no CMD0 response\n", NULL, (APTR)putch, NULL);
        return -1;
	}

    // Send CMD8 to the card
	// Voltage supplied = 0x1 = 2.7-3.6V (standard)
	// Check pattern = 10101010b (as per PLSS 4.3.13) = 0xAA

    RawDoFmt("[brcm-sdhc] note a timeout error on the following command (CMD8) is normal "
           "and expected if the SD card version is less than 2.0\n", NULL, (APTR)putch, NULL);

    SDCardBase->sd_CMD(SEND_IF_COND, 0x1aa, 500000, SDCardBase);

	int v2_later = 0;
	if(TIMEOUT(SDCardBase))
        v2_later = 0;
    else if(CMD_TIMEOUT(SDCardBase))
    {
        if(sd_reset_cmd(SDCardBase) == -1)
            return -1;
        wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, SD_ERR_MASK_CMD_TIMEOUT);
        v2_later = 0;
    }
    else if(FAIL(SDCardBase))
    {
        RawDoFmt("[brcm-sdhc] failure sending CMD8 (%08lx)\n", &SDCardBase->sd_LastInterrupt, (APTR)putch, NULL);
        return -1;
    }
    else
    {
        if(((SDCardBase->sd_Res0) & 0xfff) != 0x1aa)
        {
            RawDoFmt("[brcm-sdhc] unusable card\n", NULL, (APTR)putch, NULL);
            RawDoFmt("[brcm-sdhc] CMD8 response %08lx\n", &SDCardBase->sd_Res0, (APTR)putch, NULL);
            return -1;
        }
        else
            v2_later = 1;
    }

    // Here we are supposed to check the response to CMD5 (HCSS 3.6)
    // It only returns if the card is a SDIO card
    RawDoFmt("[brcm-sdhc] note that a timeout error on the following command (CMD5) is "
           "normal and expected if the card is not a SDIO card.\n", NULL, (APTR)putch, NULL);
    SDCardBase->sd_CMD(IO_SET_OP_COND, 0, 10000, SDCardBase);
    if(!TIMEOUT(SDCardBase))
    {
        if(CMD_TIMEOUT(SDCardBase))
        {
            if(sd_reset_cmd(SDCardBase) == -1)
                return -1;
            wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, SD_ERR_MASK_CMD_TIMEOUT);
        }
        else
        {
            RawDoFmt("[brcm-sdhc] SDIO card detected - not currently supported\n", NULL, (APTR)putch, NULL);
            return -1;
        }
    }

    // Call an inquiry ACMD41 (voltage window = 0) to get the OCR
    SDCardBase->sd_CMD(ACMD_41 | IS_APP_CMD, 0, 500000, SDCardBase);
    if(FAIL(SDCardBase))
    {
        RawDoFmt("[brcm-sdhc] inquiry ACMD41 failed\n", NULL, (APTR)putch, NULL);
        return -1;
    }

	// Call initialization ACMD41
	int card_is_busy = 1;
	while(card_is_busy)
	{
	    uint32_t v2_flags = 0;
	    if(v2_later)
	    {
	        // Set SDHC support
	        v2_flags |= (1 << 30);
	    }

	    SDCardBase->sd_CMD(ACMD_41 | IS_APP_CMD, 0x00ff8000 | v2_flags, 500000, SDCardBase);

	    if(FAIL(SDCardBase))
	    {
	        RawDoFmt("[brcm-sdhc] error issuing ACMD41\n", NULL, (APTR)putch, NULL);
	        return -1;
	    }

	    if((SDCardBase->sd_Res0) & 0x80000000)
	    {
	        // Initialization is complete
	        SDCardBase->sd_CardOCR = (SDCardBase->sd_Res0 >> 8) & 0xffff;
	        SDCardBase->sd_CardSupportsSDHC = (SDCardBase->sd_Res0 >> 30) & 0x1;

	        card_is_busy = 0;
	    }
	    else
	    {
            //RawDoFmt("[brcm-sdhc] card is busy, retrying\n", NULL, (APTR)putch, NULL);
            SDCardBase->sd_Delay(500000, SDCardBase);
	    }       
	}

    {
        ULONG args[] = {
            SDCardBase->sd_CardOCR, SDCardBase->sd_CardSupportsSDHC
        };
        RawDoFmt("[brcm-sdhc] card identified: OCR: %04lx, SDHC support: %ld\n", args, (APTR)putch, NULL);
    }

    // At this point, we know the card is definitely an SD card, so will definitely
	//  support SDR12 mode which runs at 25 MHz
    sd_switch_clock_rate(base_clock, SD_CLOCK_NORMAL, SDCardBase);

	// A small wait before the voltage switch
	SDCardBase->sd_Delay(5000, SDCardBase);

	// Send CMD2 to get the cards CID
	SDCardBase->sd_CMD(ALL_SEND_CID, 0, 500000, SDCardBase);
	if(FAIL(SDCardBase))
	{
	    RawDoFmt("SD: error sending ALL_SEND_CID\n", NULL, (APTR)putch, NULL);
	    return -1;
	}
	uint32_t card_cid_0 = SDCardBase->sd_Res0;
	uint32_t card_cid_1 = SDCardBase->sd_Res1;
	uint32_t card_cid_2 = SDCardBase->sd_Res2;
	uint32_t card_cid_3 = SDCardBase->sd_Res3;

    {
        ULONG args[] = {
            card_cid_3, card_cid_2, card_cid_1, card_cid_0
        };

        RawDoFmt("[brcm-sdhc] card CID: %08lx%08lx%08lx%08lx\n", args, (APTR)putch, NULL);
    }

#if 0
	uint32_t *dev_id = (uint32_t *)malloc(4 * sizeof(uint32_t));
	dev_id[0] = card_cid_0;
	dev_id[1] = card_cid_1;
	dev_id[2] = card_cid_2;
	dev_id[3] = card_cid_3;
	ret->bd.device_id = (uint8_t *)dev_id;
	ret->bd.dev_id_len = 4 * sizeof(uint32_t);
#endif

    SDCardBase->sd_CID[0] = card_cid_3;
    SDCardBase->sd_CID[1] = card_cid_2;
    SDCardBase->sd_CID[2] = card_cid_1;
    SDCardBase->sd_CID[3] = card_cid_0;

	// Send CMD3 to enter the data state
	SDCardBase->sd_CMD(SEND_RELATIVE_ADDR, 0, 500000, SDCardBase);
	if(FAIL(SDCardBase))
    {
        RawDoFmt("SD: error sending SEND_RELATIVE_ADDR\n", NULL, (APTR)putch, NULL);
        return -1;
    }

	uint32_t cmd3_resp = SDCardBase->sd_Res0;

	SDCardBase->sd_CardRCA = (cmd3_resp >> 16) & 0xffff;
	uint32_t crc_error = (cmd3_resp >> 15) & 0x1;
	uint32_t illegal_cmd = (cmd3_resp >> 14) & 0x1;
	uint32_t error = (cmd3_resp >> 13) & 0x1;
	uint32_t status = (cmd3_resp >> 9) & 0xf;
	uint32_t ready = (cmd3_resp >> 8) & 0x1;

    RawDoFmt("Res0: %08lx\n", &cmd3_resp, (APTR)putch, NULL);

	if(crc_error)
	{
		RawDoFmt("SD: CRC error\n", NULL, (APTR)putch, NULL);
		return -1;
	}

	if(illegal_cmd)
	{
		RawDoFmt("SD: illegal command\n", NULL, (APTR)putch, NULL);
		return -1;
	}

	if(error)
	{
		RawDoFmt("SD: generic error\n", NULL, (APTR)putch, NULL);
		return -1;
	}

	if(!ready)
	{
		RawDoFmt("SD: not ready for data\n", NULL, (APTR)putch, NULL);
		return -1;
	}

    RawDoFmt("[brcm-sdhc] RCA: %04lx\n", &SDCardBase->sd_CardRCA, (APTR)putch, NULL);

	// Now select the card (toggles it to transfer state)
	SDCardBase->sd_CMD(SELECT_CARD, SDCardBase->sd_CardRCA << 16, 500000, SDCardBase);
	if(FAIL(SDCardBase))
	{
	    RawDoFmt("SD: error sending CMD7\n", NULL, (APTR)putch, NULL);
	    return -1;
	}

    uint32_t cmd7_resp = SDCardBase->sd_Res0;
	status = (cmd7_resp >> 9) & 0xf;

	if((status != 3) && (status != 4))
	{
		RawDoFmt("SD: invalid status (%ld)\n", &status, (APTR)putch, NULL);
		return -1;
	}

	// If not an SDHC card, ensure BLOCKLEN is 512 bytes
	if(!SDCardBase->sd_CardSupportsSDHC)
	{
	    SDCardBase->sd_CMD(SET_BLOCKLEN, 512, 500000, SDCardBase);
	    if(FAIL(SDCardBase))
	    {
	        RawDoFmt("SD: error sending SET_BLOCKLEN\n", NULL, (APTR)putch, NULL);
	        return -1;
	    }
	}

	uint32_t controller_block_size = rd32(SDCardBase->sd_SDHC, EMMC_BLKSIZECNT);
	controller_block_size &= (~0xfff);
	controller_block_size |= 0x200;
	wr32(SDCardBase->sd_SDHC, EMMC_BLKSIZECNT, controller_block_size);

    // Get the cards SCR register
    SDCardBase->sd_Buffer = &SDCardBase->sd_SCR;
    SDCardBase->sd_BlocksToTransfer = 1;
    SDCardBase->sd_BlockSize = 8;

    SDCardBase->sd_CMD(SEND_SCR, 0, 500000, SDCardBase);
	SDCardBase->sd_BlockSize = 512;

	if(FAIL(SDCardBase))
	{
	    RawDoFmt("SD: error sending SEND_SCR\n", NULL, (APTR)putch, NULL);
	    return -1;
	}

	// Determine card version
	// Note that the SCR is big-endian
	uint32_t scr0 = SDCardBase->sd_SCR.scr[0];
    SDCardBase->sd_SCR.sd_version = SD_VER_UNKNOWN;
	uint32_t sd_spec = (scr0 >> (56 - 32)) & 0xf;
	uint32_t sd_spec3 = (scr0 >> (47 - 32)) & 0x1;
	uint32_t sd_spec4 = (scr0 >> (42 - 32)) & 0x1;
    SDCardBase->sd_SCR.sd_bus_widths = (scr0 >> (48 - 32)) & 0xf;
	if(sd_spec == 0)
        SDCardBase->sd_SCR.sd_version = SD_VER_1;
    else if(sd_spec == 1)
        SDCardBase->sd_SCR.sd_version = SD_VER_1_1;
    else if(sd_spec == 2)
    {
        if(sd_spec3 == 0)
            SDCardBase->sd_SCR.sd_version = SD_VER_2;
        else if(sd_spec3 == 1)
        {
            if(sd_spec4 == 0)
                SDCardBase->sd_SCR.sd_version = SD_VER_3;
            else if(sd_spec4 == 1)
                SDCardBase->sd_SCR.sd_version = SD_VER_4;
        }
    }

    RawDoFmt("SD: &scr: %08lx\n", &SDCardBase->sd_SCR.scr[0], (APTR)putch, NULL);
    {
        ULONG args[] = {
            SDCardBase->sd_SCR.scr[0],
            SDCardBase->sd_SCR.scr[1]
        };
        RawDoFmt("SD: SCR[0]: %08lx, SCR[1]: %08lx\n", args, (APTR)putch, NULL);
    }
    {
        ULONG args[] = {
            SDCardBase->sd_SCR.sd_version,
            SDCardBase->sd_SCR.sd_bus_widths
        };
        RawDoFmt("SD: SCR: version %ld, bus_widths %01lx\n", args, (APTR)putch, NULL);
    }

    if(SDCardBase->sd_SCR.sd_bus_widths & 0x4)
    {
        // Set 4-bit transfer mode (ACMD6)
        // See HCSS 3.4 for the algorithm
        RawDoFmt("SD: switching to 4-bit data mode\n", NULL, (APTR)putch, NULL);

        // Disable card interrupt in host
        uint32_t old_irpt_mask = rd32(SDCardBase->sd_SDHC, EMMC_IRPT_MASK);
        uint32_t new_iprt_mask = old_irpt_mask & ~(1 << 8);
        wr32(SDCardBase->sd_SDHC, EMMC_IRPT_MASK, new_iprt_mask);

        // Send ACMD6 to change the card's bit mode
        SDCardBase->sd_CMD(SET_BUS_WIDTH, 0x2, 500000, SDCardBase);
        if(FAIL(SDCardBase))
            RawDoFmt("SD: switch to 4-bit data mode failed\n", NULL, (APTR)putch, NULL);
        else
        {
            // Change bit mode for Host
            uint32_t control0 = rd32(SDCardBase->sd_SDHC, EMMC_CONTROL0);
            control0 |= 0x2;
            wr32(SDCardBase->sd_SDHC, EMMC_CONTROL0, control0);

            // Re-enable card interrupt in host
            wr32(SDCardBase->sd_SDHC, EMMC_IRPT_MASK, old_irpt_mask);

#ifdef EMMC_DEBUG
            RawDoFmt("SD: switch to 4-bit complete\n", NULL, (APTR)putch, NULL);
#endif
        }
    }

	RawDoFmt("SD: found a valid version %ld SD card\n", &SDCardBase->sd_SCR.sd_version, (APTR)putch, NULL);

	// Reset interrupt register
	wr32(SDCardBase->sd_SDHC, EMMC_INTERRUPT, 0xffffffff);

    return 0;
}


static int sd_ensure_data_mode(struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

	if(SDCardBase->sd_CardRCA == 0)
	{
		// Try again to initialise the card
		int ret = SDCardBase->sd_CardInit(SDCardBase);
		if(ret != 0)
			return ret;
	}

    SDCardBase->sd_CMD(SEND_STATUS, SDCardBase->sd_CardRCA << 16, 500000, SDCardBase);
    if(FAIL(SDCardBase))
    {
        RawDoFmt("SD: ensure_data_mode() error sending CMD13\n", NULL, (APTR)putch, NULL);
        SDCardBase->sd_CardRCA = 0;
        return -1;
    }

	uint32_t status = SDCardBase->sd_Res0;
	uint32_t cur_state = (status >> 9) & 0xf;

	if(cur_state == 3)
	{
		// Currently in the stand-by state - select it
		SDCardBase->sd_CMD(SELECT_CARD, SDCardBase->sd_CardRCA << 16, 500000, SDCardBase);
		if(FAIL(SDCardBase))
		{
			RawDoFmt("SD: ensure_data_mode() no response from CMD17\n", NULL, (APTR)putch, NULL);
			SDCardBase->sd_CardRCA = 0;
			return -1;
		}
	}
	else if(cur_state == 5)
	{
		// In the data transfer state - cancel the transmission
		SDCardBase->sd_CMD(STOP_TRANSMISSION, 0, 500000, SDCardBase);
		if(FAIL(SDCardBase))
		{
			RawDoFmt("SD: ensure_data_mode() no response from CMD12\n", NULL, (APTR)putch, NULL);
			SDCardBase->sd_CardRCA = 0;
			return -1;
		}

		// Reset the data circuit
		sd_reset_dat(SDCardBase);
	}
	else if(cur_state != 4)
	{
		// Not in the transfer state - re-initialise
		int ret = SDCardBase->sd_CardInit(SDCardBase);
		if(ret != 0)
			return ret;
	}

	// Check again that we're now in the correct mode
	if(cur_state != 4)
	{
		RawDoFmt("SD: ensure_data_mode() rechecking status: ", NULL, (APTR)putch, NULL);
        SDCardBase->sd_CMD(SEND_STATUS, SDCardBase->sd_CardRCA << 16, 500000, SDCardBase);
        if(FAIL(SDCardBase))
		{
			RawDoFmt("SD: ensure_data_mode() no response from CMD13\n", NULL, (APTR)putch, NULL);
			SDCardBase->sd_CardRCA = 0;
			return -1;
		}
		status = SDCardBase->sd_Res0;
		cur_state = (status >> 9) & 0xf;

		RawDoFmt("%ld\n", &cur_state, (APTR)putch, NULL);

		if(cur_state != 4)
		{
			RawDoFmt("SD: unable to initialise SD card to "
					"data mode (state %ld)\n", &cur_state, (APTR)putch, NULL);
			SDCardBase->sd_CardRCA = 0;
			return -1;
		}
	}

	return 0;
}

static int sd_do_data_command(int is_write, uint8_t *buf, uint32_t buf_size, uint32_t block_no, struct SDCardBase *SDCardBase)
{
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;

	// PLSS table 4.20 - SDSC cards use byte addresses rather than block addresses
	if(!SDCardBase->sd_CardSupportsSDHC)
		block_no *= 512;

	// This is as per HCSS 3.7.2.1
	if(buf_size < SDCardBase->sd_BlockSize)
	{
	    ULONG args[] = { buf_size, SDCardBase->sd_BlockSize};
        RawDoFmt("SD: do_data_command() called with buffer size (%ld) less than "
            "block size (%ld)\n", args, (APTR)putch, NULL);
        return -1;
	}

	SDCardBase->sd_BlocksToTransfer = buf_size / SDCardBase->sd_BlockSize;
	if(buf_size % SDCardBase->sd_BlockSize)
	{
        ULONG args[] = { buf_size, SDCardBase->sd_BlockSize };
	    RawDoFmt("SD: do_data_command() called with buffer size (%ld) not an "
            "exact multiple of block size (%ld)\n", args, (APTR)putch, NULL);
        return -1;
	}
	SDCardBase->sd_Buffer = buf;

	// Decide on the command to use
	int command;
	if(is_write)
	{
	    if(SDCardBase->sd_BlocksToTransfer > 1)
            command = WRITE_MULTIPLE_BLOCK;
        else
            command = WRITE_BLOCK;
	}
	else
    {
        if(SDCardBase->sd_BlocksToTransfer > 1)
            command = READ_MULTIPLE_BLOCK;
        else
            command = READ_SINGLE_BLOCK;
    }

	int retry_count = 0;
	int max_retries = 5;
	while(retry_count < max_retries)
	{
        SDCardBase->sd_CMD(command, block_no, 5000000, SDCardBase);

        if(SUCCESS(SDCardBase))
            break;
        else
        {
            // In the data transfer state - cancel the transmission
            SDCardBase->sd_CMD(STOP_TRANSMISSION, 0, 500000, SDCardBase);
            if(FAIL(SDCardBase))
            {
                SDCardBase->sd_CardRCA = 0;
                return -1;
            }

            // Reset the data circuit
            sd_reset_dat(SDCardBase);
            //RawDoFmt("SD: error sending CMD%ld, ", &command, (APTR)putch, NULL);
            //RawDoFmt("error = %08lx.  ", &SDCardBase->sd_LastError, (APTR)putch, NULL);
            retry_count++;
            /*
            if(retry_count < max_retries)
                RawDoFmt("Retrying...\n", NULL, (APTR)putch, NULL);
            else
                RawDoFmt("Giving up.\n", NULL, (APTR)putch, NULL);
            */
        }
	}
	if(retry_count == max_retries)
    {
        SDCardBase->sd_CardRCA = 0;
        return -1;
    }

    return 0;
}

int sd_read(uint8_t *buf, uint32_t buf_size, uint32_t block_no, struct SDCardBase *SDCardBase)
{
	// Check the status of the card
    if(sd_ensure_data_mode(SDCardBase) != 0)
        return -1;

    if(sd_do_data_command(0, buf, buf_size, block_no, SDCardBase) < 0)
        return -1;

	return buf_size;
}

int sd_write(uint8_t *buf, uint32_t buf_size, uint32_t block_no, struct SDCardBase *SDCardBase)
{
	// Check the status of the card
    if(sd_ensure_data_mode(SDCardBase) != 0)
        return -1;

    if(sd_do_data_command(1, buf, buf_size, block_no, SDCardBase) < 0)
        return -1;

	return buf_size;
}
