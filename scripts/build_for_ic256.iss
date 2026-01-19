; Inno Setup script for IC256 Sampler
; Creates a Windows installer for the application
;
; To build: iscc scripts\build_for_ic256.iss
; Or open this file in Inno Setup Compiler and click "Build"
;
; Note: AppVersion is automatically generated from pyproject.toml by build_exe.ps1
; The version.iss file is created when you run build_exe.ps1
; If building manually without build_exe.ps1, uncomment the line below and set version:
; #define AppVersion "1.0.0"

; Include version from generated file (created by build_exe.ps1)
; Fallback to default if file doesn't exist (for manual builds)
#ifndef AppVersion
  #include "version.iss"
  #ifndef AppVersion
    #define AppVersion "1.0.0"  ; Fallback if version.iss doesn't exist
  #endif
#endif

#define AppName "IC256 Sampler"
#define AppPublisher "Pyramid Technical Consultants, Inc."
#define AppURL "https://github.com/Pyramid-Technical-Consultants/ic256-sampler"
#define AppExeName "ic256-sampler.exe"
; AppId: Unique GUID for Windows installer identification
; Generate new GUIDs with: python -c "import uuid; print(str(uuid.uuid4()).upper())"
; Or PowerShell: [guid]::NewGuid()
#define AppId "{903DA095-1DB6-4F97-A2BC-B64CCE65051D}"

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
           '  .\scripts\build_exe.ps1', mbError, MB_OK);
    Result := False;
  end
  else
  begin
    Result := True;
  end;
end;
