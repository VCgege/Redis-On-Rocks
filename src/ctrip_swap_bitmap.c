/* Copyright (c) 2021, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "ctrip_swap.h"
#include "ctrip_roaring_bitmap.h"

#define SUBKEY_SIZE (4 * 1024)  /* default 4KB */

typedef struct bitmapMeta {
    int bitsNum; /* num of bits */
    roaringBitmap *subkeyInMemory;
} bitmapMeta;

bitmapMeta *bitmapMetaCreate(void) {
    bitmapMeta *bitmap_meta = zmalloc(sizeof(bitmapMeta));
    bitmap_meta->bitsNum = 0;
    bitmap_meta->subkeyInMemory = rbmCreate();
    return bitmap_meta;
}

void bitmapMetaFree(bitmapMeta *bitmap_meta) {
    if (bitmap_meta == NULL) return;
    rbmDestory(bitmap_meta->subkeyInMemory);
    zfree(bitmap_meta);
}

objectMeta *createBitmapObjectMeta(uint64_t version, bitmapMeta *bitmap_meta) {
    objectMeta *object_meta = createObjectMeta(OBJ_BITMAP, version);
    objectMetaSetPtr(object_meta, bitmap_meta);
    return object_meta;
}

#define BITMAP_META_ENCODED_LEN 4

sds encodeBitmapMeta(bitmapMeta *bm) {
    if (bm == NULL) return NULL;
    return sdsnewlen(&bm->bitsNum, sizeof(int));
}

bitmapMeta *decodeBitmapMeta(const char *extend, size_t extendLen) {
    if (extendLen != sizeof(int)) return NULL;
    bitmapMeta *bitmap_meta = bitmapMetaCreate();
    bitmap_meta->bitsNum = *(int*)extend;
}

sds encodeBitmapObjectMeta(struct objectMeta *object_meta, void *aux) {
    UNUSED(aux);
    if (object_meta == NULL) return NULL;
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    return encodeBitmapMeta(objectMetaGetPtr(object_meta));
}

int decodeBitmapObjectMeta(struct objectMeta *object_meta, const char *extend, size_t extlen) {
    serverAssert(object_meta->object_type == OBJ_BITMAP);
    serverAssert(objectMetaGetPtr(object_meta) == NULL);
    objectMetaSetPtr(object_meta, decodeBitmapMeta(extend, extlen));
    return 0;
}

int bitmapObjectMetaIsHot(struct objectMeta *object_meta, robj *value)
{
    serverAssert(value && object_meta && object_meta->object_type == OBJ_BITMAP);
    bitmapMeta *bm = objectMetaGetPtr(object_meta);
    if (bm == NULL) {
        return 1;
    } else {
        return rbmGetBitRange(bm->subkeyInMemory, 0, bm->bitsNum - 1) == 1 << bm->bitsNum;
    }
}

void bitmapObjectMetaFree(objectMeta *object_meta) {
    if (object_meta == NULL) return;
    bitmapMetaFree(objectMetaGetPtr(object_meta));
}

bitmapMeta *bitmapMetaDup(bitmapMeta *bitmap_meta) {
    bitmapMeta *bm = zmalloc(sizeof(bitmapMeta));
    bm->bitsNum = bitmap_meta->bitsNum;
    bm->subkeyInMemory = zmalloc(sizeof(roaringBitmap));
    rbmdup(bm->subkeyInMemory, bitmap_meta->subkeyInMemory);
    return bm;
}

void bitmapObjectMetaDup(struct objectMeta *dup_meta, struct objectMeta *object_meta) {
    if (object_meta == NULL) return;
    serverAssert(dup_meta->object_type == OBJ_BITMAP);
    serverAssert(objectMetaGetPtr(dup_meta) == NULL);
    if (objectMetaGetPtr(object_meta) == NULL) return;
    objectMetaSetPtr(dup_meta, bitmapMetaDup(objectMetaGetPtr(object_meta)));
}

int bitmapObjectMetaEqual(struct objectMeta *dest_om, struct objectMeta *src_om) {
    bitmapMeta *dest_bm = objectMetaGetPtr(dest_om);
    bitmapMeta *src_bm = objectMetaGetPtr(src_om);
    if (dest_bm->bitsNum != src_bm->bitsNum || !rbmIsEqual(dest_bm->subkeyInMemory, src_bm->subkeyInMemory)) {
        return 0;
    }
    return 1;
}

objectMetaType bitmapObjectMetaType = {
        .encodeObjectMeta = encodeBitmapObjectMeta,
        .decodeObjectMeta = decodeBitmapObjectMeta,
        .objectIsHot = bitmapObjectMetaIsHot,
        .free = bitmapObjectMetaFree,
        .duplicate = bitmapObjectMetaDup,
        .equal = bitmapObjectMetaEqual
};


robj *adjustBitmapSubkey(bitmapMeta *bm, robj *subkey)
{
    long long value;
    if (getLongLongFromObject(subkey,&value) != C_OK) return NULL;
    long long subkeyIndex = value / SUBKEY_SIZE;
    if (subkeyIndex != value) {
        return createStringObjectFromLongLong(subkeyIndex);
    }
    return subkey;
}

uint32_t bitmapSubkeyExists(bitmapMeta *bm, robj *subkey)
{
    long long value;
    if (getLongLongFromObject(subkey, &value) != C_OK) return NULL;
    long long subkeyIndex = value / SUBKEY_SIZE;
    return rbmGetBitRange(bm->subkeyInMemory, subkeyIndex, subkeyIndex);
}

int bitmapSwapAna(swapData *data, int thd, struct keyRequest *req,
                int *intention, uint32_t *intention_flags, void *datactx_) {
    listDataCtx *datactx = datactx_;
    int cmd_intention = req->cmd_intention;
    uint32_t cmd_intention_flags = req->cmd_intention_flags;
    UNUSED(thd);

    switch (cmd_intention) {
        case SWAP_NOP:
            *intention = SWAP_NOP;
            *intention_flags = 0;
            break;
        case SWAP_IN:
            if (!swapDataPersisted(data)) {
                /* No need to swap for pure hot key */
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else if (req->l.num_ranges == 0) {
                if (cmd_intention_flags == SWAP_IN_DEL_MOCK_VALUE) {
                    datactx->ctx_flag |= BIG_DATA_CTX_FLAG_MOCK_VALUE;
                    *intention = SWAP_DEL;
                    *intention_flags = SWAP_FIN_DEL_SKIP;
                } else {
                    /*  swap in all elements */
                    *intention = SWAP_IN;
                    *intention_flags = SWAP_EXEC_IN_DEL;
                    datactx->swap_meta = NULL;
                    if (cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                        *intention_flags |= SWAP_EXEC_FORCE_HOT;
                    }
                }
            } else { /* range requests */
                /*  swap in all elements */
                *intention = SWAP_IN;
                *intention_flags = SWAP_EXEC_IN_DEL;
                datactx->swap_meta = NULL;
                if (cmd_intention_flags & SWAP_IN_FORCE_HOT) {
                    *intention_flags |= SWAP_EXEC_FORCE_HOT;
                }
            }
            if (cmd_intention_flags & SWAP_OOM_CHECK) {
                *intention_flags |= SWAP_EXEC_OOM_CHECK;
            }
            break;
        case SWAP_OUT:
            if (swapDataIsCold(data)) {
                *intention = SWAP_NOP;
                *intention_flags = 0;
            } else {
                // todo
            }
            break;
        case SWAP_DEL:
                // todo
            break;
        default:
            break;
    }

    datactx->arg_reqs[0] = req->list_arg_rewrite[0];
    datactx->arg_reqs[1] = req->list_arg_rewrite[1];

    return 0;
}

// todo
swapDataType bitmapSwapDataType = {
        .name = "bitmap",
        .cmd_swap_flags = CMD_SWAP_DATATYPE_BITMAP,
        .swapAna = NULL,
        .swapAnaAction = NULL,
        .encodeKeys = NULL,
        .encodeData = NULL,
        .encodeRange = NULL,
        .decodeData = NULL,
        .swapIn = NULL,
        .swapOut = NULL,
        .swapDel = NULL,
        .createOrMergeObject = NULL,
        .cleanObject = NULL,
        .beforeCall = NULL,
        .free = NULL,
        .rocksDel = NULL,
        .mergedIsHot = NULL,
        .getObjectMetaAux = NULL,
};

// todo
int swapDataSetupBitmap(swapData *d, void **pdatactx) {
    d->type = &bitmapSwapDataType;
    d->omtype = &bitmapObjectMetaType;
    bitmapDataCtx *datactx = zmalloc(sizeof(bitmapDataCtx));
    *pdatactx = datactx;
    return 0;
}