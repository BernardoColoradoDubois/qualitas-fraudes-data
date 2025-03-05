# Cargar variables de entorno desde el archivo .env
function Load-EnvFile {
    param (
        [string]$envFilePath = ".\.env"
    )

    if (Test-Path $envFilePath) {
        Get-Content $envFilePath | ForEach-Object {
            if ($_ -match "^\s*([^#][^=]+)=(.*)$") {
                $name = $matches[1].Trim()
                $value = $matches[2].Trim()
                # Establecer como variable de entorno
                [Environment]::SetEnvironmentVariable($name, $value, "Process")
                Write-Verbose "Loaded environment variable: $name"
            }
        }
    } else {
        Write-Error "Environment file not found: $envFilePath"
        exit 1
    }
}

# Función para actualizar dags
function Composer-Update-Dags {
    $bucketName = $env:GCP_COMPOSER_WORK_BUCKET_NAME
    if (-not $bucketName) {
        Write-Error "GCP_COMPOSER_WORK_BUCKET_NAME environment variable is not set"
        exit 1
    }

    Write-Host "Copying DAGs to gs://$bucketName/dags/"
    gsutil cp .\dags\* gs://$bucketName/dags/
}

# Función para actualizar workspaces
function Composer-Update-Workspaces {
    $bucketName = $env:GCP_COMPOSER_WORK_BUCKET_NAME
    if (-not $bucketName) {
        Write-Error "GCP_COMPOSER_WORK_BUCKET_NAME environment variable is not set"
        exit 1
    }

    Write-Host "Copying workspaces to gs://$bucketName/workspaces/"
    gsutil cp .\workspaces\* gs://$bucketName/workspaces/
}

# Función para actualizar todo
function Composer-Update-All {
    Composer-Update-Dags
    Composer-Update-Workspaces
}

# Menú de ayuda para mostrar las funciones disponibles
function Show-Help {
    Write-Host "Funciones disponibles:" -ForegroundColor Yellow
    Write-Host "  Load-EnvFile [-envFilePath <path>]  - Carga variables de entorno desde archivo .env"
    Write-Host "  Composer-Update-Dags                - Actualiza los DAGs en el bucket de trabajo"
    Write-Host "  Composer-Update-Workspaces          - Actualiza los workspaces en el bucket de trabajo"
    Write-Host "  Composer-Update-All                 - Actualiza tanto DAGs como workspaces"
    Write-Host "  Show-Help                           - Muestra esta ayuda"
}

# Cargar automáticamente el archivo .env al importar el script
Load-EnvFile

# Exportar las funciones para que estén disponibles al importar el script
Export-ModuleMember -Function Load-EnvFile, Composer-Update-Dags, Composer-Update-Workspaces, Composer-Update-All, Show-Help
