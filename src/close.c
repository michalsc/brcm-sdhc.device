#include <exec/types.h>
#include <exec/libraries.h>
#include <exec/io.h>

#include "sdcard.h"

ULONG SD_Close(struct IORequest * io asm("a1"))
{
    struct SDCardBase *SDCardBase = (struct SDCardBase *)io->io_Device;
    struct SDCardUnit *u = (struct SDCardUnit *)io->io_Unit;

    u->su_Unit.unit_OpenCnt--;
    SDCardBase->sd_Device.dd_Library.lib_OpenCnt--;

    if (SDCardBase->sd_Device.dd_Library.lib_OpenCnt == 0)
    {
        if (SDCardBase->sd_Device.dd_Library.lib_Flags & LIBF_DELEXP)
        {
            return SD_Expunge(SDCardBase);
        }
    }
    
    return 0;
}
