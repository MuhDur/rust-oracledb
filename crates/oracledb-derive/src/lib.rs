//! Procedural macros for the `oracledb` driver.
//!
//! This crate hosts the `#[derive(FromRow)]` macro. It is an implementation
//! detail of the `oracledb` crate: end users never depend on it directly. The
//! `oracledb` crate re-exports the derive (gated behind its default-on `derive`
//! feature) alongside the `FromRow` trait the generated code implements, so the
//! single import
//!
//! ```ignore
//! use oracledb::FromRow;
//! ```
//!
//! brings both the trait and the derive into scope.
//!
//! # What the derive generates
//!
//! For a struct
//!
//! ```ignore
//! #[derive(FromRow)]
//! struct Emp { id: i64, name: String, hired: Option<chrono::NaiveDate> }
//! ```
//!
//! the macro emits an `impl oracledb::FromRow for Emp`, whose `from_row` pulls
//! each field out of the row **by column name** through the existing typed
//! accessor (`TypedRow::get_by_name`), which itself goes through the real
//! `FromSql` conversion. There is no stringly-typed shortcut: an `i64` field is
//! genuinely converted from the Oracle `NUMBER`, an `Option<T>` field accepts a
//! SQL `NULL`, and a `chrono::NaiveDate` field is decoded from a `DATE`.
//!
//! # Supported shapes and attributes
//!
//! * Named-field structs — each field maps to a column named after the field.
//! * Tuple structs — each field maps to a column **by position** (index 0, 1, …).
//! * `#[oracledb(rename_all = "UPPERCASE" | "lowercase" | "snake_case" |
//!   "SCREAMING_SNAKE_CASE" | "camelCase" | "PascalCase")]` on the struct
//!   renames every field's column (column resolution is case-insensitive, so
//!   `UPPERCASE`/`lowercase` are cosmetic, but the others change word casing).
//! * `#[oracledb(column = "…")]` on a field overrides that one column name.
//! * `#[oracledb(rename = "…")]` is an accepted alias for `column = "…"`.
//!
//! Enums, unions, and unit/zero-field structs produce a clear `compile_error!`.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Fields, LitStr, Meta, Token};

/// Derive an `oracledb::FromRow` implementation that maps a query row into the
/// annotated struct, with compile-time-checked field types.
///
/// See the [crate-level documentation](crate) for the supported shapes and the
/// `#[oracledb(...)]` attributes.
#[proc_macro_derive(FromRow, attributes(oracledb))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// One of the case-conversion strategies accepted by `rename_all`.
#[derive(Clone, Copy)]
enum RenameAll {
    Upper,
    Lower,
    Snake,
    ScreamingSnake,
    Camel,
    Pascal,
}

impl RenameAll {
    fn parse(value: &LitStr) -> syn::Result<Self> {
        Ok(match value.value().as_str() {
            "UPPERCASE" => RenameAll::Upper,
            "lowercase" => RenameAll::Lower,
            "snake_case" => RenameAll::Snake,
            "SCREAMING_SNAKE_CASE" => RenameAll::ScreamingSnake,
            "camelCase" => RenameAll::Camel,
            "PascalCase" => RenameAll::Pascal,
            other => {
                return Err(syn::Error::new(
                    value.span(),
                    format!(
                        "unknown rename_all rule {other:?}; expected one of \
                         \"UPPERCASE\", \"lowercase\", \"snake_case\", \
                         \"SCREAMING_SNAKE_CASE\", \"camelCase\", \"PascalCase\""
                    ),
                ));
            }
        })
    }

    fn apply(self, field: &str) -> String {
        match self {
            RenameAll::Upper => field.to_uppercase(),
            RenameAll::Lower => field.to_lowercase(),
            RenameAll::Snake => to_snake(field),
            RenameAll::ScreamingSnake => to_snake(field).to_uppercase(),
            RenameAll::Camel => to_camel(field, false),
            RenameAll::Pascal => to_camel(field, true),
        }
    }
}

/// Convert an identifier (`fooBar`, `FooBar`, `foo_bar`) to `foo_bar`.
fn to_snake(field: &str) -> String {
    let mut out = String::with_capacity(field.len() + 4);
    let mut prev_lower_or_digit = false;
    for ch in field.chars() {
        if ch.is_uppercase() {
            if prev_lower_or_digit {
                out.push('_');
            }
            for lower in ch.to_lowercase() {
                out.push(lower);
            }
            prev_lower_or_digit = false;
        } else {
            out.push(ch);
            prev_lower_or_digit = ch.is_lowercase() || ch.is_ascii_digit();
        }
    }
    out
}

/// Convert an identifier to camelCase (`pascal = false`) or PascalCase
/// (`pascal = true`), treating `_` as a word boundary.
fn to_camel(field: &str, pascal: bool) -> String {
    let mut out = String::with_capacity(field.len());
    let mut upper_next = pascal;
    for ch in field.chars() {
        if ch == '_' {
            upper_next = true;
            continue;
        }
        if upper_next {
            for upper in ch.to_uppercase() {
                out.push(upper);
            }
            upper_next = false;
        } else {
            out.push(ch);
        }
    }
    out
}

/// Container-level options parsed from `#[oracledb(...)]` on the struct.
#[derive(Default)]
struct ContainerOpts {
    rename_all: Option<RenameAll>,
}

/// Field-level options parsed from `#[oracledb(...)]` on a field.
#[derive(Default)]
struct FieldOpts {
    column: Option<String>,
}

/// Parse every `#[oracledb(...)]` attribute on a struct into container options.
fn parse_container_opts(input: &DeriveInput) -> syn::Result<ContainerOpts> {
    let mut opts = ContainerOpts::default();
    for attr in &input.attrs {
        if !attr.path().is_ident("oracledb") {
            continue;
        }
        let metas =
            attr.parse_args_with(syn::punctuated::Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match &meta {
                Meta::NameValue(nv) if nv.path.is_ident("rename_all") => {
                    let lit = expect_str_lit(&nv.value)?;
                    opts.rename_all = Some(RenameAll::parse(&lit)?);
                }
                _ => {
                    return Err(syn::Error::new(
                        meta.span(),
                        "unsupported #[oracledb(...)] container option; \
                         expected `rename_all = \"...\"`",
                    ));
                }
            }
        }
    }
    Ok(opts)
}

/// Parse every `#[oracledb(...)]` attribute on a field into field options.
fn parse_field_opts(attrs: &[syn::Attribute]) -> syn::Result<FieldOpts> {
    let mut opts = FieldOpts::default();
    for attr in attrs {
        if !attr.path().is_ident("oracledb") {
            continue;
        }
        let metas =
            attr.parse_args_with(syn::punctuated::Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            match &meta {
                Meta::NameValue(nv) if nv.path.is_ident("column") || nv.path.is_ident("rename") => {
                    let lit = expect_str_lit(&nv.value)?;
                    opts.column = Some(lit.value());
                }
                _ => {
                    return Err(syn::Error::new(
                        meta.span(),
                        "unsupported #[oracledb(...)] field option; \
                         expected `column = \"...\"` or `rename = \"...\"`",
                    ));
                }
            }
        }
    }
    Ok(opts)
}

/// If `ty` is syntactically `Option<Inner>` (in any of the spellings
/// `Option<T>`, `std::option::Option<T>`, `core::option::Option<T>`), return
/// `Some(Inner)`. This drives the choice between the NULL-rejecting accessor and
/// the NULL-tolerant one in the generated code: an `Option<T>` field accepts a
/// SQL `NULL` (mapping to `None`), a bare `T` field does not.
fn option_inner(ty: &syn::Type) -> Option<&syn::Type> {
    let syn::Type::Path(type_path) = ty else {
        return None;
    };
    if type_path.qself.is_some() {
        return None;
    }
    let segments = &type_path.path.segments;
    // Accept `Option`, `option::Option`, `std::option::Option`, etc. — the last
    // segment must be `Option` and the leading segments (if any) must be the
    // `std`/`core` option path.
    let last = segments.last()?;
    if last.ident != "Option" {
        return None;
    }
    let ok_prefix = match segments.len() {
        1 => true,
        n => {
            let lead: Vec<String> = segments
                .iter()
                .take(n - 1)
                .map(|s| s.ident.to_string())
                .collect();
            let lead: Vec<&str> = lead.iter().map(String::as_str).collect();
            matches!(
                lead.as_slice(),
                ["option"] | ["std", "option"] | ["core", "option"]
            )
        }
    };
    if !ok_prefix {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

/// Require an expression to be a string literal, returning it.
fn expect_str_lit(expr: &syn::Expr) -> syn::Result<LitStr> {
    if let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(s),
        ..
    }) = expr
    {
        Ok(s.clone())
    } else {
        Err(syn::Error::new(expr.span(), "expected a string literal"))
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let data = match &input.data {
        Data::Struct(data) => data,
        Data::Enum(e) => {
            return Err(syn::Error::new(
                e.enum_token.span(),
                "#[derive(FromRow)] cannot be applied to enums: a query row maps \
                 to a fixed set of columns, which an enum's variants do not model",
            ));
        }
        Data::Union(u) => {
            return Err(syn::Error::new(
                u.union_token.span(),
                "#[derive(FromRow)] cannot be applied to unions",
            ));
        }
    };

    let container = parse_container_opts(&input)?;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let body = match &data.fields {
        Fields::Named(named) => named_body(named, &container)?,
        Fields::Unnamed(unnamed) => unnamed_body(unnamed)?,
        Fields::Unit => {
            return Err(syn::Error::new(
                input.ident.span(),
                "#[derive(FromRow)] needs at least one field to map; a unit struct \
                 maps no columns",
            ));
        }
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::oracledb::FromRow for #name #ty_generics #where_clause {
            fn from_row(
                row: &::oracledb::TypedRow<'_>,
            ) -> ::core::result::Result<Self, ::oracledb::ConversionError> {
                ::core::result::Result::Ok(#name #body)
            }
        }
    })
}

/// Build the `{ field: ..., ... }` construction for a named-field struct.
fn named_body(named: &syn::FieldsNamed, container: &ContainerOpts) -> syn::Result<TokenStream2> {
    if named.named.is_empty() {
        return Err(syn::Error::new(
            named.span(),
            "#[derive(FromRow)] needs at least one field to map",
        ));
    }
    let mut inits = Vec::with_capacity(named.named.len());
    for field in &named.named {
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new(field.span(), "named field without an identifier"))?;
        let ty = &field.ty;
        let opts = parse_field_opts(&field.attrs)?;
        let column = match opts.column {
            Some(explicit) => explicit,
            None => {
                let base = ident.to_string();
                let base = base.strip_prefix("r#").unwrap_or(&base);
                match container.rename_all {
                    Some(rule) => rule.apply(base),
                    None => base.to_string(),
                }
            }
        };
        let access = match option_inner(ty) {
            // Option<Inner>: NULL -> None. Call the NULL-tolerant accessor with
            // the *inner* type so it returns Option<Inner> == the field type.
            Some(inner) => quote! { row.try_get_by_name_opt::<#inner>(#column)? },
            // Bare T: NULL is an error.
            None => quote! { row.try_get_by_name::<#ty>(#column)? },
        };
        inits.push(quote! { #ident: #access });
    }
    Ok(quote! { { #(#inits),* } })
}

/// Build the `( ..., ... )` construction for a tuple struct, mapping by position.
fn unnamed_body(unnamed: &syn::FieldsUnnamed) -> syn::Result<TokenStream2> {
    if unnamed.unnamed.is_empty() {
        return Err(syn::Error::new(
            unnamed.span(),
            "#[derive(FromRow)] needs at least one field to map",
        ));
    }
    let mut inits = Vec::with_capacity(unnamed.unnamed.len());
    for (index, field) in unnamed.unnamed.iter().enumerate() {
        // Reject field-level attributes that only make sense on named fields.
        let opts = parse_field_opts(&field.attrs)?;
        if opts.column.is_some() {
            return Err(syn::Error::new(
                field.span(),
                "#[oracledb(column = ...)] is not supported on a tuple struct field; \
                 tuple-struct fields map to columns by position",
            ));
        }
        let ty = &field.ty;
        let access = match option_inner(ty) {
            Some(inner) => quote! { row.try_get_opt::<#inner>(#index)? },
            None => quote! { row.try_get::<#ty>(#index)? },
        };
        inits.push(access);
    }
    // Trailing comma so a single-field tuple struct constructs as a tuple
    // (`Foo(x,)`) rather than a parenthesized expression (`Foo(x)`).
    Ok(quote! { ( #(#inits,)* ) })
}
