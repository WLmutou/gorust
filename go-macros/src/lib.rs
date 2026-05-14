use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, Type, parse_macro_input};

mod select_parse;
use select_parse::parse_select;


/// Mark the main function to use the GoRust runtime
///
/// # Example
/// ```rust
/// use gorust::{runtime, go, make_chan};
///
/// #[runtime]
/// fn main() {
///     go(|| {
///         debug!("Hello from goroutine!");
///     });
///     
///     let ch = make_chan!(i32, 10);
///     go(move || {
///         ch.send(42).unwrap();
///     });
///     
///     debug!("Received: {}", ch.recv().unwrap());
/// }
/// ```
#[proc_macro_attribute]
pub fn runtime(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(input as ItemFn);
    let _fn_name = &input_fn.sig.ident;
    let fn_block = &input_fn.block;
    let fn_attrs = &input_fn.attrs;
    let fn_vis = &input_fn.vis;
    let fn_sig = &input_fn.sig;

    // 生成包装后的 main 函数
    let expanded = quote! {
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            ::gorust::Runtime::init();

            let original_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |panic_info| {
                println!("[Goroutine panic] {}", panic_info);
                original_hook(panic_info);
            }));

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #fn_block
            }));

            ::gorust::Runtime::wait_for_all();
            ::gorust::Runtime::wait_and_shutdown();
            ::gorust::Runtime::shutdown();

            match result {
                Ok(ret) => ret,
                Err(err) => std::panic::resume_unwind(err),
            }
        }
    };

    TokenStream::from(expanded)
}

/// make_chan! macro - Create a channel
#[proc_macro]
pub fn make_chan(input: TokenStream) -> TokenStream {
    let parsed_input = input.to_string();
    let parts: Vec<&str> = parsed_input.trim_end_matches(')').split(',').collect();

    if parts.len() == 1 {
        // 无缓冲 channel
        let ty = parts[0].trim();
        let ty_parsed: Type = syn::parse_str(ty).expect("Invalid type in make_chan!");
        let expanded = quote! {
            ::gorust::Channel::<#ty_parsed>::new(0)
        };
        TokenStream::from(expanded)
    } else {
        // 有缓冲 channel
        let ty = parts[0].trim();
        let ty_parsed: Type = syn::parse_str(ty).expect("Invalid type in make_chan!");
        let size = parts[1].trim();
        // 尝试解析容量为表达式
        let size_expr: proc_macro2::TokenStream =
            size.parse().expect("Invalid capacity in make_chan!");
        let expanded = quote! {
            ::gorust::Channel::<#ty_parsed>::new(#size_expr)
        };
        TokenStream::from(expanded)
    }
}

/// select! macro - Go 风格的 select 语法
///
/// # Example
/// ```rust
/// use gorust::{select, go, make_chan};
/// use std::time::Duration;
///
/// let ch = make_chan!(i32, 10);
///
/// select! {
///     val <- ch => {
///         println!("Got: {}", val);
///     },
///     timeout!(Duration::from_secs(2)) => {
///         println!("Timeout!");
///     },
///     default => {
///         println!("No data");
///     }
/// }
/// ```
#[proc_macro]
pub fn select(input: TokenStream) -> TokenStream {
    let input_str = input.to_string();
    match parse_select(input_str) {
        Ok(expanded) => TokenStream::from(expanded),
        Err(err) => {
            let error = quote! {
                compile_error!(#err)
            };
            TokenStream::from(error)
        }
    }
}


