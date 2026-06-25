/*
 * tdc/plugin.h
 *
 * Runtime registration of user-defined model, transform, and entropy
 * backends. Registered vtables are visible to tdc_model_get,
 * tdc_xform_get, and tdc_entropy_get alongside the built-in core
 * backends.
 *
 * Id range enforcement:
 *   0x0001-0x00FF   core (statically compiled; registration rejected)
 *   0x0100-0x01FF   experimental (statically compiled; registration rejected)
 *   0x0200-0xFEFF   reserved (registration rejected)
 *   0xFF00-0xFFFF   user-defined (this is the only registrable range)
 *
 * Thread safety: NONE. All registration calls must happen before any
 * concurrent encode/decode. Once a vtable is registered, the pointer
 * must remain valid for the lifetime of the process (or until
 * tdc_plugin_clear is called).
 *
 * Capacity: each stage holds up to TDC_PLUGIN_MAX_SLOTS user-defined
 * backends. Exceeding the limit returns TDC_E_NOMEM.
 */

#ifndef TDC_PLUGIN_H
#define TDC_PLUGIN_H

#include "types.h"
#include "model.h"
#include "transform.h"
#include "entropy.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TDC_PLUGIN_MAX_SLOTS 16

/* Id range for user-defined backends. */
#define TDC_PLUGIN_ID_MIN 0xFF00u
#define TDC_PLUGIN_ID_MAX 0xFFFFu

/*
 * Register a user-defined backend vtable.
 *
 * Returns:
 *   TDC_OK          — registered successfully
 *   TDC_E_INVAL     — id outside 0xFF00-0xFFFF, or vt is NULL,
 *                      or vt->id does not match the id argument,
 *                      or id is already registered for this stage
 *   TDC_E_NOMEM     — all TDC_PLUGIN_MAX_SLOTS slots are taken
 */
tdc_status tdc_model_register(tdc_model_id id, const tdc_model_vt *vt);
tdc_status tdc_xform_register(tdc_xform_id id, const tdc_xform_vt *vt);
tdc_status tdc_entropy_register(tdc_entropy_id id, const tdc_entropy_vt *vt);

/*
 * Remove all user-defined registrations for all stages.
 * Intended for test teardown; not needed in normal usage.
 */
void tdc_plugin_clear(void);

#ifdef __cplusplus
}
#endif
#endif /* TDC_PLUGIN_H */
