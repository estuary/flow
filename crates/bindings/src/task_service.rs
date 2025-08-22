use prost::Message;

type TaskServiceImpl = runtime::TaskService;

// Opaque pointer for a TaskServer instance in the ABI.
#[repr(C)]
pub struct TaskServiceImplPtr {
    _private: [u8; 0],
}

#[repr(C)]
pub struct TaskService {
    // Opaque service pointer.
    svc_ptr: *mut TaskServiceImplPtr,

    // Terminal error returned by the TaskService.
    err_ptr: *mut u8,
    err_len: usize,
    err_cap: usize,
}

#[unsafe(no_mangle)]
pub extern "C" fn new_task_service(config_ptr: *const u8, config_len: u32) -> *mut TaskService {
    let config = unsafe { std::slice::from_raw_parts(config_ptr, config_len as usize) };
    let config = proto_flow::runtime::TaskServiceConfig::decode(config).unwrap();

    let log_file = unsafe {
        use std::os::unix::io::FromRawFd;
        std::fs::File::from_raw_fd(config.log_file_fd)
    };

    let svc_abi = match runtime::TaskService::new(config, log_file) {
        Ok(svc) => {
            let svc_ptr =
                Box::leak(Box::new(svc)) as *mut TaskServiceImpl as *mut TaskServiceImplPtr;

            TaskService {
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

            TaskService {
                svc_ptr: 0 as *mut TaskServiceImplPtr,
                err_ptr,
                err_len,
                err_cap,
            }
        }
    };

    Box::leak(Box::new(svc_abi))
}

#[unsafe(no_mangle)]
pub extern "C" fn task_service_drop(svc: *mut TaskService) {
    let TaskService {
        svc_ptr,
        err_ptr,
        err_len,
        err_cap,
    } = *unsafe { Box::from_raw(svc) };

    if svc_ptr != 0 as *mut TaskServiceImplPtr {
        let svc = unsafe { Box::from_raw(svc_ptr as *mut TaskServiceImpl) };
        svc.graceful_stop();
    }
    unsafe { String::from_raw_parts(err_ptr, err_len, err_cap) };
}
