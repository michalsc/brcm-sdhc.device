#include <exec/types.h>

#include "sdcard.h"

APTR SD_ExtFunc(struct SDCardBase * SDCardBase asm("a6"))
{
    return SDCardBase;
}
