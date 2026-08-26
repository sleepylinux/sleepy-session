use sleepy_sdk::SemanticAction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActionStatement {
    Native(&'static str),
    Spawn(&'static [&'static str]),
}

const TERMINAL: &[&str] = &["ghostty"];
const LAUNCHER: &[&str] = &["fuzzel"];
const CONTROL_CENTER: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "toggleControlCenter",
];
const SESSION_LOCK: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "requestSessionAction",
    "lock",
];
const SESSION_LOGOUT: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "requestSessionAction",
    "logout",
];
const SESSION_REBOOT: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "requestSessionAction",
    "reboot",
];
const SESSION_POWER_OFF: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "requestSessionAction",
    "powerOff",
];
const OPEN_POWER_MENU: &[&str] = &[
    "quickshell",
    "ipc",
    "--config",
    "sleepy",
    "call",
    "sleepy",
    "openPowerMenu",
];
const MEDIA_PLAY_PAUSE: &[&str] = &["playerctl", "play-pause"];
const MEDIA_NEXT: &[&str] = &["playerctl", "next"];
const MEDIA_PREVIOUS: &[&str] = &["playerctl", "previous"];
const VOLUME_UP: &[&str] = &["wpctl", "set-volume", "@DEFAULT_AUDIO_SINK@", "5%+"];
const VOLUME_DOWN: &[&str] = &["wpctl", "set-volume", "@DEFAULT_AUDIO_SINK@", "5%-"];
const VOLUME_MUTE: &[&str] = &["wpctl", "set-mute", "@DEFAULT_AUDIO_SINK@", "toggle"];
const MICROPHONE_MUTE: &[&str] = &["wpctl", "set-mute", "@DEFAULT_AUDIO_SOURCE@", "toggle"];
const BRIGHTNESS_UP: &[&str] = &["brightnessctl", "set", "5%+"];
const BRIGHTNESS_DOWN: &[&str] = &["brightnessctl", "set", "5%-"];

pub(super) const fn statement(action: SemanticAction) -> ActionStatement {
    match action {
        SemanticAction::TerminalOpen => ActionStatement::Spawn(TERMINAL),
        SemanticAction::LauncherOpen => ActionStatement::Spawn(LAUNCHER),
        SemanticAction::WindowClose => ActionStatement::Native("close-window"),
        SemanticAction::WindowFocusLeft => ActionStatement::Native("focus-column-left"),
        SemanticAction::WindowFocusRight => ActionStatement::Native("focus-column-right"),
        SemanticAction::WindowFocusUp => ActionStatement::Native("focus-window-up"),
        SemanticAction::WindowFocusDown => ActionStatement::Native("focus-window-down"),
        SemanticAction::WorkspacePrevious => ActionStatement::Native("focus-workspace-up"),
        SemanticAction::WorkspaceNext => ActionStatement::Native("focus-workspace-down"),
        SemanticAction::ControlCenterToggle => ActionStatement::Spawn(CONTROL_CENTER),
        SemanticAction::SessionLock => ActionStatement::Spawn(SESSION_LOCK),
        SemanticAction::SessionLogout => ActionStatement::Spawn(SESSION_LOGOUT),
        SemanticAction::SessionReboot => ActionStatement::Spawn(SESSION_REBOOT),
        SemanticAction::SessionPowerOff => ActionStatement::Spawn(SESSION_POWER_OFF),
        SemanticAction::SessionPower => ActionStatement::Spawn(OPEN_POWER_MENU),
        SemanticAction::MediaPlayPause => ActionStatement::Spawn(MEDIA_PLAY_PAUSE),
        SemanticAction::MediaNext => ActionStatement::Spawn(MEDIA_NEXT),
        SemanticAction::MediaPrevious => ActionStatement::Spawn(MEDIA_PREVIOUS),
        SemanticAction::VolumeUp => ActionStatement::Spawn(VOLUME_UP),
        SemanticAction::VolumeDown => ActionStatement::Spawn(VOLUME_DOWN),
        SemanticAction::VolumeToggleMute => ActionStatement::Spawn(VOLUME_MUTE),
        SemanticAction::MicrophoneToggleMute => ActionStatement::Spawn(MICROPHONE_MUTE),
        SemanticAction::BrightnessUp => ActionStatement::Spawn(BRIGHTNESS_UP),
        SemanticAction::BrightnessDown => ActionStatement::Spawn(BRIGHTNESS_DOWN),
    }
}
