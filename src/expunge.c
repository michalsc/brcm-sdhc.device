#include <exec/types.h>
#include <exec/libraries.h>

#include "sdcard.h"

ULONG SD_Expunge(struct SDCardBase * SDCardBase asm("a6"))
{
    /* This is rom based device. no expunge here */
    if (SDCardBase->sd_Device.dd_Library.lib_OpenCnt > 0)
    {
        SDCardBase->sd_Device.dd_Library.lib_Flags |= LIBF_DELEXP;
    }

    return 0;
}
