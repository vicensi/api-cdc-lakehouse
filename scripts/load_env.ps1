# Carrega o .env na sessão atual do PowerShell (o dbt não lê .env sozinho).
# Uso, a partir da raiz do projeto:   . .\scripts\load_env.ps1
Get-Content (Join-Path $PSScriptRoot "..\.env") | ForEach-Object {
    if ($_ -match '^\s*([^#=\s][^=]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
    }
}
# Diz ao dbt onde está o profiles.yml (a pasta dbt/ deste projeto)
$env:DBT_PROFILES_DIR = Join-Path (Split-Path $PSScriptRoot -Parent) "dbt"
Write-Host "variáveis carregadas. profiles do dbt em: $env:DBT_PROFILES_DIR"
