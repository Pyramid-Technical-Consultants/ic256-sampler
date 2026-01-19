; Inno Setup script for IC256 Sampler
; Creates a Windows installer for the application
;
; To build: iscc setup\BuildForIC256.iss
; Or open this file in Inno Setup Compiler and click "Build"

#define AppName "IC256 Sampler"
#define AppVersion "1.0.0"
#define AppPublisher "IC256 Sampler Team"
#define AppURL "https://github.com/Pyramid-Technical-Consultants/ic256-sampler"
#define AppExeName "ic256-sampler.exe"
#define AppId "{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}"

[Setup]
; App identification
AppId={#AppId}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
AllowNoIcons=yes

; Output
OutputDir=..\dist
OutputBaseFilename=IC256-Sampler-Setup-{#AppVersion}
SetupIconFile=logo.ico
UninstallDisplayIcon={app}\{#AppExeName}

; Compression
Compression=lzma2
SolidCompression=yes
LZMAUseSeparateProcess=yes
LZMANumFastBytes=64

; Architecture
ArchitecturesInstallIn64BitMode=x64
ArchitecturesAllowed=x64compatible

; Privileges
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog

; License
LicenseFile=..\LICENSE

; Version info
VersionInfoVersion={#AppVersion}
VersionInfoCompany={#AppPublisher}
VersionInfoDescription={#AppName}
VersionInfoCopyright=Copyright (C) 2024 {#AppPublisher}
VersionInfoProductName={#AppName}
VersionInfoProductVersion={#AppVersion}

; UI
WizardStyle=modern
WizardImageFile=
WizardSmallImageFile=
DisableWelcomePage=no
DisableDirPage=no
DisableProgramGroupPage=no
DisableReadyPage=no
DisableFinishedPage=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Dirs]
Name: "{app}\Data"; Permissions: users-full

[Files]
; Main executable
Source: "..\dist\{#AppExeName}"; DestDir: "{app}"; Flags: ignoreversion; Permissions: users-full; Check: FileExists(ExpandConstant('..\dist\{#AppExeName}'))
; Icon file
Source: "logo.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Start Menu
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\logo.ico"
Name: "{group}\{cm:UninstallProgram,{#AppName}}"; Filename: "{uninstallexe}"
; Desktop
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\logo.ico"; Tasks: desktopicon
; Quick Launch (Windows 7 and earlier)
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#AppName}"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\logo.ico"; Tasks: quicklaunchicon

[Run]
Filename: "{app}\{#AppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(AppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\Data"

[Code]
// Pre-installation check to ensure executable exists
function InitializeSetup(): Boolean;
var
  ExePath: String;
begin
  ExePath := ExpandConstant('..\dist\{#AppExeName}');
  if not FileExists(ExePath) then
  begin
    MsgBox('Executable not found: ' + ExePath + #13#10 + #13#10 + 
           'Please build the executable first using:' + #13#10 +
           '  .\setup\build-exe.ps1', mbError, MB_OK);
    Result := False;
  end
  else
  begin
    Result := True;
  end;
end;
