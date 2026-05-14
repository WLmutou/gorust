// src/macros/select_parse.rs - 支持 Go 风格的 select 语法

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

pub enum SelectCaseKind {
    Recv(Pat, Expr),
    Send(Expr, Expr),
    Timeout(Expr),
    Default,
}

pub struct SelectCase {
    pub kind: SelectCaseKind,
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

    if lookahead.peek(Token![default]) {
        input.parse::<Token![default]>()?;
        input.parse::<FatArrow>()?;
        let body = input.parse::<Expr>()?;

        return Ok(SelectCase {
            kind: SelectCaseKind::Default,
            body,
        });
    }

    if lookahead.peek(syn::Ident) {
        let fork = input.fork();
        if let Ok(ident) = fork.parse::<syn::Ident>() {
            if ident == "timeout" || ident == "After" {
                input.parse::<syn::Ident>()?;
                input.parse::<Token![!]>()?;
                let content;
                syn::parenthesized!(content in input);
                let duration = content.parse::<Expr>()?;
                input.parse::<FatArrow>()?;
                let body = input.parse::<Expr>()?;

                return Ok(SelectCase {
                    kind: SelectCaseKind::Timeout(duration),
                    body,
                });
            }
        }
    }

    let fork = input.fork();
    if let Ok(_pat) = Pat::parse_single(&fork) {
        if fork.peek(Token![<-]) {
            let pat = Pat::parse_single(input)?;
            input.parse::<Token![<-]>()?;
            let channel = input.parse::<Expr>()?;
            input.parse::<FatArrow>()?;
            let body = input.parse::<Expr>()?;

            return Ok(SelectCase {
                kind: SelectCaseKind::Recv(pat, channel),
                body,
            });
        }
    }

    let expr = input.parse::<Expr>()?;

    if let Expr::MethodCall(method) = &expr {
        if method.method == "send" {
            input.parse::<FatArrow>()?;
            let body = input.parse::<Expr>()?;

            let channel = *method.receiver.clone();
            let value = method.args.first().cloned().ok_or_else(|| {
                syn::Error::new(method.method.span(), "send() requires an argument")
            })?;

            return Ok(SelectCase {
                kind: SelectCaseKind::Send(channel, value),
                body,
            });
        }
    }

    Err(syn::Error::new(
        input.span(),
        "Expected: pat <- chan => body, chan.send(val) => body, timeout!(dur) => body, or default => body",
    ))
}

pub fn parse_select(input_str: String) -> Result<TokenStream2, String> {
    let parse_result = syn::parse_str::<SelectInput>(&input_str);

    match parse_result {
        Ok(select_input) => {
            let has_default = select_input
                .cases
                .iter()
                .any(|c| matches!(c.kind, SelectCaseKind::Default));
            let has_timeout = select_input
                .cases
                .iter()
                .any(|c| matches!(c.kind, SelectCaseKind::Timeout(_)));
            Ok(generate_select_impl(select_input.cases, has_default, has_timeout))
        }
        Err(err) => Err(format!("Parse error: {}", err)),
    }
}

fn generate_select_impl(cases: Vec<SelectCase>, has_default: bool, has_timeout: bool) -> TokenStream2 {
    if has_default {
        generate_non_blocking_select(cases)
    } else {
        generate_blocking_select(cases, has_timeout)
    }
}

fn generate_non_blocking_select(cases: Vec<SelectCase>) -> TokenStream2 {
    let mut checks = Vec::new();
    let mut default_body = None;

    for case in cases {
        match case.kind {
            SelectCaseKind::Recv(pat, chan) => {
                let body = case.body;
                checks.push(quote! {
                    if let Ok(val) = #chan.try_recv() {
                        let #pat = val;
                        #body
                        return;
                    }
                });
            }
            SelectCaseKind::Send(chan, val) => {
                let body = case.body;
                checks.push(quote! {
                    if #chan.try_send(#val).is_ok() {
                        #body
                        return;
                    }
                });
            }
            SelectCaseKind::Default => {
                default_body = Some(case.body);
            }
            SelectCaseKind::Timeout(_) => {
                continue;
            }
        }
    }

    quote! {
        {
            #(#checks)*
            #default_body
        }
    }
}

fn generate_blocking_select(cases: Vec<SelectCase>, _has_timeout: bool) -> TokenStream2 {
    let mut recv_tokens = Vec::new();
    let mut send_tokens = Vec::new();
    let mut timeout_tokens = Vec::new();
    let mut branches = Vec::new();
    let mut case_counter = 0usize;

    for case in &cases {
        match &case.kind {
            SelectCaseKind::Recv(pat, chan) => {
                let body = &case.body;
                let case_id = case_counter;
                recv_tokens.push(quote! {
                    {
                        let __tx = __result_tx.clone();
                        let __chan = #chan.clone();
                        let __case_id = #case_id;
                        ::gorust::go(move || {
                            if let Some(__val) = __chan.recv() {
                                let _ = __tx.send((__case_id, Box::new(__val)));
                            }
                        });
                    }
                });
                branches.push((case_id, pat.clone(), body.clone(), true));
                case_counter += 1;
            }
            SelectCaseKind::Send(chan, val) => {
                let body = &case.body;
                let case_id = case_counter;
                send_tokens.push(quote! {
                    {
                        let __tx = __result_tx.clone();
                        let __chan = #chan.clone();
                        let __val = #val.clone();
                        let __case_id = #case_id;
                        ::gorust::go(move || {
                            if __chan.send(__val).is_ok() {
                                let _ = __tx.send((__case_id, Box::new(())));
                            }
                        });
                    }
                });
                branches.push((case_id, syn::parse_quote!(_), body.clone(), false));
                case_counter += 1;
            }
            SelectCaseKind::Timeout(duration) => {
                let body = &case.body;
                let case_id = case_counter;
                let dur = duration;
                timeout_tokens.push(quote! {
                    {
                        let __tx = __result_tx.clone();
                        let __case_id = #case_id;
                        ::gorust::go(move || {
                            std::thread::sleep(#dur);
                            let _ = __tx.send((__case_id, Box::new(())));
                        });
                    }
                });
                branches.push((case_id, syn::parse_quote!(_), body.clone(), false));
                case_counter += 1;
            }
            SelectCaseKind::Default => {
                continue;
            }
        }
    }

    let branch_matches: Vec<_> = branches.iter().map(|(case_id, pat, body, is_recv)| {
        if *is_recv {
            quote! {
                #case_id => {
                    let #pat = *Box::<dyn std::any::Any>::downcast(__val).unwrap();
                    #body
                }
            }
        } else {
            quote! {
                #case_id => {
                    #body
                }
            }
        }
    }).collect();

    quote! {
        {
            use std::sync::mpsc::channel;

            let (__result_tx, __result_rx): (
                std::sync::mpsc::Sender<(usize, Box<dyn std::any::Any>)>,
                std::sync::mpsc::Receiver<(usize, Box<dyn std::any::Any>)>
            ) = channel();

            #(#recv_tokens)*
            #(#send_tokens)*
            #(#timeout_tokens)*

            if let Ok((__case_id, __val)) = __result_rx.recv() {
                match __case_id {
                    #(#branch_matches)*
                    _ => {}
                }
            }

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

    #[test]
    fn test_parse_select_with_timeout() {
        let input = r#"
            val <- ch1 => {
                println!("Got: {}", val);
            },
            timeout!(std::time::Duration::from_secs(2)) => {
                println!("超时！取消等待");
            }
        "#;

        let result = parse_select(input.to_string());
        assert!(result.is_ok());
    }
}
