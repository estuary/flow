use prost::Message;

// Opaque pointer for a TaskService instance in the ABI.
#[repr(C)]
pub struct TaskServiceV2ImplPtr {
    _private: [u8; 0],
}

#[repr(C)]
pub struct TaskServiceV2 {
    // Opaque service pointer.
    svc_ptr: *mut TaskServiceV2ImplPtr,

    // Terminal error returned by the TaskService.
    err_ptr: *mut u8,
    err_len: usize,
    err_cap: usize,
}

#[unsafe(no_mangle)]
pub extern "C" fn new_task_service_v2(
    config_ptr: *const u8,
    config_len: u32,
) -> *mut TaskServiceV2 {
    let config = unsafe { std::slice::from_raw_parts(config_ptr, config_len as usize) };
    let config = proto_flow::runtime::TaskServiceConfig::decode(config).unwrap();

    let log_file = unsafe {
        use std::os::unix::io::FromRawFd;
        std::fs::File::from_raw_fd(config.log_file_fd)
    };

    let svc_abi = match runtime_next::TaskService::new(config, log_file) {
        Ok(svc) => {
            let svc_ptr = Box::leak(Box::new(svc)) as *mut runtime_next::TaskService
                as *mut TaskServiceV2ImplPtr;

            TaskServiceV2 {
                svc_ptr,
                err_ptr: 0 as *mut u8,
                err_len: 0,
                err_cap: 0,
            }
        }
        Err(err) => {
            let mut err = format!("{:?}", err);
            let err_ptr = err.as_mut_ptr();
            let err_cap = err.capacity();
            let err_len = err.len();
            std::mem::forget(err);

            TaskServiceV2 {
                svc_ptr: 0 as *mut TaskServiceV2ImplPtr,
                err_ptr,
                err_len,
                err_cap,
            }
        }
    };

    Box::leak(Box::new(svc_abi))
}

#[unsafe(no_mangle)]
pub extern "C" fn task_service_v2_drop(svc: *mut TaskServiceV2) {
    let TaskServiceV2 {
        svc_ptr,
        err_ptr,
        err_len,
        err_cap,
    } = *unsafe { Box::from_raw(svc) };

    if svc_ptr != 0 as *mut TaskServiceV2ImplPtr {
        let svc = unsafe { Box::from_raw(svc_ptr as *mut runtime_next::TaskService) };
        svc.graceful_stop();
    }
    let err_ptr = if err_cap == 0 {
        std::ptr::NonNull::dangling().as_ptr()
    } else {
        err_ptr
    };
    unsafe { String::from_raw_parts(err_ptr, err_len, err_cap) };
}
