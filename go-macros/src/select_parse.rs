// src/macros/select_parse.rs - 修复括号匹配问题

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Expr, Pat, Token,
    parse::{Parse, ParseStream},
    token::{Comma, FatArrow},
};

pub struct SelectInput {
    pub cases: Vec<SelectCase>,
}

pub struct SelectCase {
    pub recv: Option<(Pat, Expr)>,
    pub send: Option<(Expr, Expr)>,
    pub body: Expr,
}

impl Parse for SelectInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut cases = Vec::new();

        while !input.is_empty() {
            let case = parse_select_case(input)?;
            cases.push(case);

            if input.peek(Comma) {
                input.parse::<Comma>()?;
            }
        }

        Ok(SelectInput { cases })
    }
}

fn parse_select_case(input: ParseStream) -> syn::Result<SelectCase> {
    let lookahead = input.lookahead1();

    // 检查是否是 default case
    if lookahead.peek(Token![default]) {
        input.parse::<Token![default]>()?;
        input.parse::<FatArrow>()?;
        let body = input.parse::<Expr>()?;

        return Ok(SelectCase {
            recv: None,
            send: None,
            body,
        });
    }

    // 尝试解析模式 (pattern <- channel)
    let fork = input.fork();

    // 使用 parse_single 方法来解析模式，注意传递引用
    if let Ok(_pat) = Pat::parse_single(&fork) {
        if fork.peek(Token![<-]) {
            // 确实是接收操作
            let pat = Pat::parse_single(input)?;
            input.parse::<Token![<-]>()?;
            let channel = input.parse::<Expr>()?;
            input.parse::<FatArrow>()?;
            let body = input.parse::<Expr>()?;

            return Ok(SelectCase {
                recv: Some((pat, channel)),
                send: None,
                body,
            });
        }
    }

    // 尝试解析发送操作 (channel.send(value))
    let expr_span = input.span(); // 保存原始输入的位置，以便错误报告
    let expr = input.parse::<Expr>()?;

    if let Expr::MethodCall(method) = &expr {
        // 使用引用避免移动
        if method.method == "send" {
            input.parse::<FatArrow>()?;
            let body = input.parse::<Expr>()?;

            let channel = *method.receiver.clone();
            let value = method.args.first().cloned().ok_or_else(|| {
                syn::Error::new(method.method.span(), "send() requires an argument")
            })?;

            return Ok(SelectCase {
                recv: None,
                send: Some((channel, value)),
                body,
            });
        }
    }

    // 使用保存的位置来报告错误
    Err(syn::Error::new(
        expr_span,
        "Expected pattern <- channel or channel.send(value)",
    ))
}

pub fn parse_select(input_str: String) -> Result<TokenStream2, String> {
    let parse_result = syn::parse_str::<SelectInput>(&input_str);

    match parse_result {
        Ok(select_input) => {
            let has_default = select_input
                .cases
                .iter()
                .any(|c| c.recv.is_none() && c.send.is_none());
            Ok(generate_select_impl(select_input.cases, has_default))
        }
        Err(err) => Err(format!("Parse error: {}", err)),
    }
}

fn generate_select_impl(cases: Vec<SelectCase>, has_default: bool) -> TokenStream2 {
    if has_default {
        generate_non_blocking_select(cases)
    } else {
        generate_blocking_select(cases)
    }
}

fn generate_non_blocking_select(cases: Vec<SelectCase>) -> TokenStream2 {
    let mut checks = Vec::new();
    let mut default_body = None;

    for case in cases {
        match (case.recv, case.send) {
            (Some((pat, chan)), None) => {
                let body = case.body;
                checks.push(quote! {
                    if let Ok(val) = #chan.try_recv() {
                        let #pat = val;
                        #body
                        return;
                    }
                });
            }
            (None, Some((chan, val))) => {
                let body = case.body;
                checks.push(quote! {
                    if #chan.try_send(#val).is_ok() {
                        #body
                        return;
                    }
                });
            }
            (None, None) => {
                default_body = Some(case.body);
            }
            _ => unreachable!(),
        }
    }

    quote! {
        {
            use ::gorust::Selectable;
            #(#checks)*
            #default_body
        }
    }
}

// 修复：使用一个简单的select实现，基于通道等待
fn generate_blocking_select(cases: Vec<SelectCase>) -> TokenStream2 {
    let recv_tokens: Vec<_> = cases
        .iter()
        .enumerate()
        .filter_map(|(i, case)| {
            if let Some((_pat, chan)) = &case.recv {
                let _body = &case.body;
                Some(quote! {
                    {
                        let __tx = __result_tx.clone();
                        let __chan = #chan.clone();
                        let __case_id = #i;
                        ::gorust::go(move || {
                            if let Some(__val) = __chan.recv() {
                                // 发送case id和接收到的值
                                let _ = __tx.send((__case_id, Ok(__val)));
                            }
                        });
                    }
                })
            } else {
                None
            }
        })
        .collect();

    let send_tokens: Vec<_> = cases
        .iter()
        .enumerate()
        .filter_map(|(i, case)| {
            if let Some((chan, val)) = &case.send {
                let _body = &case.body;
                Some(quote! {
                    {
                        let __tx = __result_tx.clone();
                        let __chan = #chan.clone();
                        let __val = #val.clone();
                        let __case_id = #i;
                        ::gorust::go(move || {
                            if __chan.send(__val).is_ok() {
                                // 发送case id和单位值
                                let _ = __tx.send((__case_id, Err(())));
                            }
                        });
                    }
                })
            } else {
                None
            }
        })
        .collect();

    let branches: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, case)| {
            match (&case.recv, &case.send) {
                (Some((pat, _)), None) => {
                    let body = &case.body;
                    quote! {
                        #i => {
                            if let Ok(__val) = __result_val {
                                let #pat = __val;
                                #body
                            }
                        }
                    }
                }
                (None, Some(_)) => {
                    let body = &case.body;
                    quote! {
                        #i => {
                            let _ = __result_val; // 忽略发送结果
                            #body
                        }
                    }
                }
                _ => quote! {}, // 这种情况不应该发生
            }
        })
        .collect();

    // 修复类型推断问题：明确指定通道元素的类型
    quote! {
        {
            use std::sync::mpsc::channel;
            use ::gorust::Selectable;

            let (__result_tx, __result_rx): (std::sync::mpsc::Sender<(usize, Result<_, ()>)>,
                                            std::sync::mpsc::Receiver<(usize, Result<_, ()>)>) = channel();

            // 启动所有接收操作的goroutine
            #(#recv_tokens)*

            // 启动所有发送操作的goroutine
            #(#send_tokens)*

            // 接收第一个完成的结果
            if let Ok((__case_id, __result_val)) = __result_rx.recv() {
                match __case_id {
                    #(#branches)*
                    _ => {}
                }
            }

            // 清理资源
            drop(__result_tx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_basic() {
        let input = r#"
            val <- ch1 => {
                println!("Got: {}", val);
            }
        "#;

        let result = parse_select(input.to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_with_send() {
        let input = r#"
            ch2.send(42) => {
                println!("Sent!");
            }
        "#;

        let result = parse_select(input.to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_with_default() {
        let input = r#"
            val <- ch1 => {
                println!("Got: {}", val);
            },
            default => {
                println!("No op");
            }
        "#;

        let result = parse_select(input.to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_select_multiple_cases() {
        let input = r#"
            val1 <- ch1 => {
                println!("From ch1: {}", val1);
            },
            val2 <- ch2 => {
                println!("From ch2: {}", val2);
            },
            ch3.send(42) => {
                println!("Sent to ch3");
            }
        "#;

        let result = parse_select(input.to_string());
        assert!(result.is_ok());
    }
}