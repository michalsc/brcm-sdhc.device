#include <exec/types.h>
#include <exec/devices.h>
#include <exec/io.h>
#include <exec/execbase.h>
#include <exec/errors.h>
#include <devices/trackdisk.h>

#include <proto/exec.h>

#include "sdcard.h"

void SD_Open(struct IORequest * io asm("a1"), LONG unitNumber asm("d0"), ULONG flags asm("d1"))
{
    struct SDCardBase *SDCardBase = (struct SDCardBase *)io->io_Device;
    struct ExecBase *SysBase = SDCardBase->sd_SysBase;
    (void)flags;

    if (SDCardBase->sd_Verbose > 1)
    {
        ULONG args[] = {
            (ULONG)io, unitNumber, flags
        };
        RawDoFmt("[brcm-sdhc] Open(%08lx, %lx, %ld)\n", args, (APTR)putch, NULL);
    }

    /* Do not continue if unit number does not fit */
    if (unitNumber >= SDCardBase->sd_UnitCount || (unitNumber == 0 && SDCardBase->sd_HideUnit0)) {
        io->io_Error = TDERR_BadUnitNum;
        return;
    }

    /*
        Do whatever necessary to open given unit number with flags, set NT_REPLYMSG if 
        opening device shall complete with success, set io_Error otherwise
    */
    io->io_Error = 0;

    /* Get unit based on unit number */
    struct SDCardUnit *u = SDCardBase->sd_Units[unitNumber];

    /* Increase open counter of the unit */
    u->su_Unit.unit_OpenCnt++;

    /* Increase global open coutner of the device */
    SDCardBase->sd_Device.dd_Library.lib_OpenCnt++;
    SDCardBase->sd_Device.dd_Library.lib_Flags &= ~LIBF_DELEXP;

    io->io_Unit = &u->su_Unit;
    io->io_Unit->unit_flags |= UNITF_ACTIVE;
    io->io_Message.mn_Node.ln_Type = NT_REPLYMSG;

    /* In contrast to normal library there is no need to return anything */
}
