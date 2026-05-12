pub mod builder;
pub mod event;

pub use builder::{SessionBuilder, SessionBuilderError};
pub use event::SessionEvent;

use crate::cache_session::CacheSession;
use crate::context::Context;
use crate::message::{InboundMessage, Message, OutboundMessage};
use crate::util::get_last_error_info;
use crate::SessionError;
use crate::SolClientReturnCode;
use solace_rs_sys::{self as ffi, solClient_opaqueMsg_pt};
use std::ffi::CString;
use std::marker::PhantomData;
use std::num::NonZeroU32;
use tracing::warn;

type Result<T> = std::result::Result<T, SessionError>;

/// Type-erased owner for the flow-event closure `Box<Box<F>>`. The closure
/// must live as long as the Session because the C library retains a pointer
/// into it as `user_p` and invokes it on flow events. Carrying it as a
/// separate generic parameter on `Session` would ripple through `CacheSession`,
/// so we erase the type and remember how to drop it.
pub(crate) struct FlowFnHolder {
    ptr: *mut (),
    drop_fn: unsafe fn(*mut ()),
}

// Safety: the underlying `Box<Box<F>>` requires F: Send (enforced by the
// builder's bound on OnFlowEvent), and the holder owns it exclusively.
unsafe impl Send for FlowFnHolder {}

impl FlowFnHolder {
    /// Wrap a `Box<Box<F>>` and return both the holder and a `*mut Box<F>`
    /// suitable for passing as `user_p`. The holder must outlive any callback
    /// invocation that uses the pointer.
    pub(crate) fn new<F>(func: Box<Box<F>>) -> (Self, *mut Box<F>) {
        let raw = Box::into_raw(func);
        unsafe fn drop_fn<F>(ptr: *mut ()) {
            // SAFETY: ptr came from Box::into_raw::<Box<F>>.
            drop(unsafe { Box::from_raw(ptr as *mut Box<F>) });
        }
        (
            Self {
                ptr: raw as *mut (),
                drop_fn: drop_fn::<F>,
            },
            raw,
        )
    }
}

impl Drop for FlowFnHolder {
    fn drop(&mut self) {
        unsafe { (self.drop_fn)(self.ptr) };
    }
}

/// Flow configuration for queue subscriptions
#[derive(Clone, Copy, Default)]
pub struct FlowConfig {
    /// Flow window size (default 255). Higher values allow more throughput.
    pub window_size: Option<u32>,
    /// ACK timer in ms (default 1000).
    pub ack_timer_ms: Option<u32>,
    /// ACK threshold (default 60).
    pub ack_threshold: Option<u32>,
}

pub struct Session<
    'session,
    M: FnMut(InboundMessage) + Send + 'session,
    E: FnMut(SessionEvent) + Send + 'session,
> {
    pub(crate) lifetime: PhantomData<&'session ()>,

    // Pointer to session
    // This pointer must never be allowed to leave the struct
    pub(crate) _session_ptr: ffi::solClient_opaqueSession_pt,
    pub(crate) _flow_p: ffi::solClient_opaqueFlow_pt,
    // The `context` field is never accessed, but implicitly does
    // reference counting via the `Drop` trait.
    #[allow(dead_code)]
    pub(crate) context: Context,

    // These fields are used to store the fn callback. The mutable reference to this fn is passed to the FFI library,
    #[allow(dead_code, clippy::redundant_allocation)]
    _msg_fn_ptr: Option<Box<Box<M>>>,
    #[allow(dead_code, clippy::redundant_allocation)]
    _event_fn_ptr: Option<Box<Box<E>>>,
    #[allow(dead_code, clippy::redundant_allocation)]
    _flow_func_info: ffi::solClient_flow_createFuncInfo_t,
    #[allow(dead_code)]
    pub(crate) _flow_fn_holder: Option<FlowFnHolder>,

    /// Flow configuration for queue subscriptions
    pub(crate) flow_config: FlowConfig,
}

unsafe impl<M: FnMut(InboundMessage) + Send, E: FnMut(SessionEvent) + Send> Send
    for Session<'_, M, E>
{
}

impl<'session, M: FnMut(InboundMessage) + Send, E: FnMut(SessionEvent) + Send>
    Session<'session, M, E>
{
    pub fn publish(&self, message: OutboundMessage) -> Result<()> {
        let send_message_raw_rc = unsafe {
            ffi::solClient_session_sendMsg(self._session_ptr, message.get_raw_message_ptr())
        };

        let rc = SolClientReturnCode::from_raw(send_message_raw_rc);
        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::PublishError(rc, subcode));
        }

        Ok(())
    }

    pub fn subscribe<T>(&self, topic: T) -> Result<()>
    where
        T: Into<Vec<u8>>,
    {
        let c_topic = CString::new(topic)?;
        let subscription_raw_rc =
            unsafe { ffi::solClient_session_topicSubscribe(self._session_ptr, c_topic.as_ptr()) };

        let rc = SolClientReturnCode::from_raw(subscription_raw_rc);

        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::SubscriptionFailure(
                c_topic.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }
        Ok(())
    }

    pub fn unsubscribe<T>(&self, topic: T) -> Result<()>
    where
        T: Into<Vec<u8>>,
    {
        let c_topic = CString::new(topic)?;
        let subscription_raw_rc =
            unsafe { ffi::solClient_session_topicUnsubscribe(self._session_ptr, c_topic.as_ptr()) };

        let rc = SolClientReturnCode::from_raw(subscription_raw_rc);

        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::UnsubscriptionFailure(
                c_topic.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }
        Ok(())
    }

    pub fn subscribe_queue<T>(&mut self, queue: T) -> Result<()>
    where
        T: Into<Vec<u8>>,
    {
        if unsafe {
            ffi::solClient_session_isCapable(
                self._session_ptr,
                ffi::SOLCLIENT_SESSION_CAPABILITY_ENDPOINT_MANAGEMENT.as_ptr() as *const i8,
            )
        } == 0
        {
            return Err(SessionError::QueueSubscriptionFailure(String::from(
                "Endpoint management not supported on this appliance.",
            )));
        }

        let c_queue = CString::new(queue)?;

        // Provision Queue
        let mut prov_props: [*const i8; 5] = [
            ffi::SOLCLIENT_ENDPOINT_PROP_ID.as_ptr() as *const i8,
            ffi::SOLCLIENT_ENDPOINT_PROP_QUEUE.as_ptr() as *const i8,
            ffi::SOLCLIENT_ENDPOINT_PROP_NAME.as_ptr() as *const i8,
            c_queue.as_ptr(),
            std::ptr::null(),
        ];

        let subscription_raw_rc = unsafe {
            ffi::solClient_session_endpointProvision(
                prov_props.as_mut_ptr(),
                self._session_ptr,
                ffi::SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM
                    | ffi::SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        let rc = SolClientReturnCode::from_raw(subscription_raw_rc);
        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::SubscriptionFailure(
                c_queue.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }

        // Convert flow config values to CStrings (need to live until flow is created)
        let window_size_str = self
            .flow_config
            .window_size
            .map(|v| CString::new(v.to_string()).unwrap());
        let ack_timer_str = self
            .flow_config
            .ack_timer_ms
            .map(|v| CString::new(v.to_string()).unwrap());
        let ack_threshold_str = self
            .flow_config
            .ack_threshold
            .map(|v| CString::new(v.to_string()).unwrap());

        tracing::info!(
            "Flow config: window_size={:?}, ack_timer_ms={:?}, ack_threshold={:?}",
            self.flow_config.window_size,
            self.flow_config.ack_timer_ms,
            self.flow_config.ack_threshold
        );

        // Set up Flow - build props dynamically based on config
        let mut flow_props: Vec<*const i8> = vec![
            ffi::SOLCLIENT_FLOW_PROP_BIND_ENTITY_ID.as_ptr() as *const i8,
            ffi::SOLCLIENT_FLOW_PROP_BIND_ENTITY_QUEUE.as_ptr() as *const i8,
            ffi::SOLCLIENT_FLOW_PROP_ACKMODE.as_ptr() as *const i8,
            ffi::SOLCLIENT_FLOW_PROP_ACKMODE_AUTO.as_ptr() as *const i8,
            ffi::SOLCLIENT_FLOW_PROP_BIND_NAME.as_ptr() as *const i8,
            c_queue.as_ptr(),
        ];

        if let Some(ref s) = window_size_str {
            flow_props.push(ffi::SOLCLIENT_FLOW_PROP_WINDOWSIZE.as_ptr() as *const i8);
            flow_props.push(s.as_ptr());
        }
        if let Some(ref s) = ack_timer_str {
            flow_props.push(ffi::SOLCLIENT_FLOW_PROP_ACK_TIMER_MS.as_ptr() as *const i8);
            flow_props.push(s.as_ptr());
        }
        if let Some(ref s) = ack_threshold_str {
            flow_props.push(ffi::SOLCLIENT_FLOW_PROP_ACK_THRESHOLD.as_ptr() as *const i8);
            flow_props.push(s.as_ptr());
        }

        flow_props.push(std::ptr::null());

        let mut flow_func_info = self._flow_func_info; // copy
        let flow_raw_rc = unsafe {
            ffi::solClient_session_createFlow(
                flow_props.as_mut_ptr(),
                self._session_ptr,
                &mut self._flow_p,
                &mut flow_func_info,
                std::mem::size_of::<ffi::solClient_flow_createFuncInfo_t>(),
            )
        };

        let rc = SolClientReturnCode::from_raw(flow_raw_rc);
        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::SubscriptionFailure(
                c_queue.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }
        Ok(())
    }

    pub fn unsubscribe_queue<T>(&mut self, queue: T) -> Result<()>
    where
        T: Into<Vec<u8>>,
    {
        if unsafe {
            ffi::solClient_session_isCapable(
                self._session_ptr,
                ffi::SOLCLIENT_SESSION_CAPABILITY_ENDPOINT_MANAGEMENT.as_ptr() as *const i8,
            )
        } == 0
        {
            return Err(SessionError::QueueSubscriptionFailure(String::from(
                "Endpoint management not supported on this appliance.",
            )));
        }
        let c_queue = CString::new(queue)?;
        // Remove Flow
        let rc = unsafe { ffi::solClient_flow_destroy(&mut self._flow_p as *mut _) };
        let rc = SolClientReturnCode::from_raw(rc);
        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::UnsubscriptionFailure(
                c_queue.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }
        // Deprovision Queue
        let subscription_raw_rc = unsafe {
            ffi::solClient_session_endpointDeprovision(
                &mut c_queue.as_ptr(),
                self._session_ptr,
                ffi::SOLCLIENT_PROVISION_FLAGS_WAITFORCONFIRM
                    | ffi::SOLCLIENT_PROVISION_FLAGS_IGNORE_EXIST_ERRORS,
                std::ptr::null_mut(),
            )
        };
        let rc = SolClientReturnCode::from_raw(subscription_raw_rc);
        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::UnsubscriptionFailure(
                c_queue.to_string_lossy().into_owned(),
                rc,
                subcode,
            ));
        }

        Ok(())
    }

    pub fn request(
        &self,
        message: OutboundMessage,
        timeout_ms: NonZeroU32,
    ) -> Result<InboundMessage> {
        let mut reply_ptr: solClient_opaqueMsg_pt = std::ptr::null_mut();

        let rc = unsafe {
            ffi::solClient_session_sendRequest(
                self._session_ptr,
                message.get_raw_message_ptr(),
                &mut reply_ptr,
                timeout_ms.into(),
            )
        };

        let rc = SolClientReturnCode::from_raw(rc);

        if !rc.is_ok() {
            // reply_ptr is always set to null if rc is not Ok
            // https://docs.solace.com/API-Developer-Online-Ref-Documentation/c/sol_client_8h.html#ac00adf1a9301ebe67fd0790523d5a44b
            debug_assert!(reply_ptr.is_null());

            let subcode = get_last_error_info();
            return Err(SessionError::RequestError(rc, subcode));
        }

        debug_assert!(!reply_ptr.is_null());

        let reply = InboundMessage::from(reply_ptr);

        Ok(reply)
    }

    pub fn cache_session<N>(
        self,
        cache_name: N,
        max_message: Option<u64>,
        max_age: Option<u64>,
        timeout_ms: Option<u64>,
    ) -> Result<CacheSession<'session, M, E>>
    where
        N: Into<Vec<u8>>,
    {
        CacheSession::new(self, cache_name, max_message, max_age, timeout_ms)
    }

    pub fn disconnect(self) -> Result<()> {
        let rc = unsafe { ffi::solClient_session_disconnect(self._session_ptr) };

        let rc = SolClientReturnCode::from_raw(rc);

        if !rc.is_ok() {
            let subcode = get_last_error_info();
            return Err(SessionError::DisconnectError(rc, subcode));
        }
        Ok(())
    }
}

impl<M: FnMut(InboundMessage) + Send, E: FnMut(SessionEvent) + Send> Drop for Session<'_, M, E> {
    fn drop(&mut self) {
        let session_free_result = unsafe { ffi::solClient_session_destroy(&mut self._session_ptr) };
        let rc = SolClientReturnCode::from_raw(session_free_result);

        if !rc.is_ok() {
            warn!("session was not dropped properly. {rc}");
        }
    }
}
