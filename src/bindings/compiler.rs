use std::fmt::Write;

use sleepy_sdk::{
    packaged_reserved_keybindings, validate_keybindings_with_reserved, PresetDocument,
    SemanticAction,
};

use super::{actions, actions::ActionStatement, BindingError};

/// Compiles a validated semantic preset into a complete generated Niri include.
pub fn compile_bindings(preset: &PresetDocument) -> Result<String, BindingError> {
    let actions = preset
        .keybindings
        .keys()
        .map(|identifier| {
            SemanticAction::try_from(identifier.as_str()).map_err(|_| {
                BindingError::new(
                    "unknown_semantic_action",
                    format!("unknown semantic action {identifier}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_keybindings_with_reserved(&preset.keybindings, &packaged_reserved_keybindings())
        .map_err(BindingError::keybinding_conflict)?;

    let mut output = String::from("binds {\n");
    for ((_, accelerator), action) in preset.keybindings.iter().zip(actions) {
        write!(&mut output, "    {accelerator} {{ ").expect("writing to String cannot fail");
        render_statement(&mut output, actions::statement(action));
        output.push_str(" }\n");
    }
    output.push_str("}\n");
    Ok(output)
}

fn render_statement(output: &mut String, statement: ActionStatement) {
    match statement {
        ActionStatement::Native(action) => {
            output.push_str(action);
            output.push(';');
        }
        ActionStatement::Spawn(arguments) => {
            output.push_str("spawn");
            for argument in arguments {
                output.push(' ');
                write_kdl_string(output, argument);
            }
            output.push(';');
        }
    }
}

fn write_kdl_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                write!(output, "\\u{{{:x}}}", character as u32)
                    .expect("writing to String cannot fail");
            }
            character => output.push(character),
        }
    }
    output.push('"');
}
