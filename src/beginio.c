/*
    Copyright © 2021 Michal Schulz <michal.schulz@gmx.de>
    https://github.com/michalsc

    This Source Code Form is subject to the terms of the
    Mozilla Public License, v. 2.0. If a copy of the MPL was not distributed
    with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

#include <exec/types.h>
#include <exec/resident.h>
#include <exec/io.h>
#include <exec/devices.h>
#include <exec/errors.h>
#include <dos/dosextens.h>

#include <devices/newstyle.h>
#include <devices/trackdisk.h>
#include <devices/scsidisk.h>

#include <proto/exec.h>

#include "sdcard.h"

static const ULONG quick = 
    (1 << TD_CHANGENUM)     |
    (1 << TD_GETDRIVETYPE)  |
    (1 << TD_GETGEOMETRY)   |
    (1 << TD_CHANGESTATE)   |
    (1 << TD_ADDCHANGEINT)  |
    (1 << TD_REMCHANGEINT)  |
    (1 << TD_MOTOR)         |
    (1 << TD_PROTSTATUS);

void SD_BeginIO(struct IORequest *io asm("a1"))
{
    struct SDCardBase *SDCardBase = (struct SDCardBase *)io->io_Device;
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;
    struct SDCardUnit *unit = (struct SDCardUnit *)io->io_Unit;
    struct IOStdReq *std = (struct IOStdReq *)io;

    io->io_Error = 0;
    io->io_Message.mn_Node.ln_Type = NT_MESSAGE;

    if (SDCardBase->sd_Verbose > 1)
    {
        bug("[brcm-sdhc:%ld] BeginIO Unit=%08lx, cmd=%ld, length=%ld, actual=%08lx, offset=%08lx\n", 
            unit->su_UnitNum,
            (ULONG)io->io_Unit,
            io->io_Command,
            std->io_Length,
            std->io_Actual,
            std->io_Offset);
    }

    Disable();

    /* Check if command is quick. If this is the case, process immediately */
    if (io->io_Command == NSCMD_DEVICEQUERY || 
        ((io->io_Command < 32) && ((1 << io->io_Command) & quick)))
    {
        Enable();
        switch(io->io_Command)
        {
            case NSCMD_DEVICEQUERY: /* Fallthrough */
            case TD_CHANGENUM:      /* Fallthrough */
            case TD_GETDRIVETYPE:   /* Fallthrough */
            case TD_GETNUMTRACKS:   /* Fallthrough */
            case TD_GETGEOMETRY:    /* Fallthrough */
            case TD_CHANGESTATE:    /* Fallthrough */
            case TD_ADDCHANGEINT:   /* Fallthrough */
            case TD_REMCHANGEINT:   /* Fallthrough */
            case TD_MOTOR:          /* Fallthrough */
            case TD_PROTSTATUS:
                SDCardBase->sd_DoIO(io, SDCardBase);
                break;
            default:
                io->io_Error = IOERR_NOCMD;
                break;
        }

        /* 
            The IOF_QUICK flag was cleared. It means the caller was going to wait for command 
            completion. Therefore, reply the command now.
        */
        if (!(io->io_Flags & IOF_QUICK))
            ReplyMsg(&io->io_Message);
    }
    else
    {
        /* 
            If command is slow, clear IOF_QUICK flag and put it to some internal queue. When
            done with slow command, use ReplyMsg to notify exec that the command completed.
            In such case *do not* reply the command now.
            When modifying IoRequest, do it in disabled state.
            In case of a quick command, handle it now.
        */
        io->io_Flags &= ~IOF_QUICK;
        Enable();

        /* Push the command to a queue, process it maybe in another task/process, reply when
        completed */
        PutMsg(&unit->su_Unit.unit_MsgPort, &io->io_Message);
    }
}
