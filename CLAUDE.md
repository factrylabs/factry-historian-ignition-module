# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Factry Historian Module** for Inductive Automation's Ignition platform. It's an Ignition SDK module that implements a custom historian with two main components:
- **Collector (Storage Provider)**: Writes tag data to external storage via REST API
- **Provider (History Provider)**: Reads historical data from external storage and provides it to Ignition

The module integrates with Ignition's Tag History system and supports standard historian features like deadbands, sample modes, and DataSet organization.

## Ignition Version Targeting

**IMPORTANT**: This module targets **Ignition 8.3+** exclusively. Do not maintain backward compatibility with earlier versions.

Key implications:
- **Use 8.3+ APIs**: The module system changed significantly in Ignition 8.3. Always reference 8.3+ documentation and examples.
- **Module Dependencies**: Use the newer `moduleDependencySpecs` block (not `moduleDependencies`) to leverage the "required" flag feature introduced in 8.3
- **SDK Version**: Using SDK 8.3.1
- **Deprecated APIs**: Avoid any APIs or patterns marked as deprecated or designed for pre-8.3 compatibility

When searching for documentation or examples, always specify "Ignition 8.3" or later to avoid outdated patterns from the 8.1/8.2 era.

## Java Version Requirement

**CRITICAL**: This project requires **Java 17** to build and run.

The codebase is configured with Java 17 toolchain in all `build.gradle.kts` files. Both the build toolchain and Ignition 8.3.1 runtime use Java 17.

To verify your Java version:
```bash
java -version  # Should show Java 17
```

Java versions other than 17 may cause build or runtime compatibility issues.

## Build Commands

### Basic Build
```bash
# Clean build (requires Java 17)
./gradlew clean build

# Build without signing (for development)
# Edit build.gradle.kts and set: skipModlSigning.set(true)
./gradlew build
```

### Module Signing
The module requires signing certificates configured in `gradle.properties`:
```properties
ignition.signing.keystoreFile=certificates/keystore.jks
ignition.signing.keystorePassword=<password>
ignition.signing.certAlias=<alias>
ignition.signing.certFile=certificates/cert.p7b
ignition.signing.certPassword=<password>
```

**IMPORTANT**: Never commit `gradle.properties` with real passwords to git.

For development, set `skipModlSigning.set(true)` in `build.gradle.kts` (line 120).

### Build Output
- Signed module: `build/Factry-Historian.modl`
- Unsigned module: `build/Factry-Historian.unsigned.modl`

## Project Architecture

### Module Structure

The module has a single subproject:

- **`:gateway`** (Scope: G - Gateway only)
   - All historian logic: storage, queries, gRPC communication
   - Hook: `FactryHistorianGatewayHook`
   - Implements Storage Provider and History Provider interfaces

### Module Configuration

Module metadata is defined in root `build.gradle.kts`:
- **Module ID**: `io.factry.FactryHistorian`
- **Display Name**: Factry Historian
- **Ignition SDK Version**: 8.3.1
- **Target Ignition Version**: 8.3+ (not backward compatible with 8.1/8.2)
- **Required Ignition Version**: 8.3.0

Scope mappings in `projectScopes`:
- Gateway: "G"

**Note**: This module is designed exclusively for Ignition 8.3+ and uses the new Historian Extension Point API introduced in 8.3.

### Hook Classes

The gateway hook extends `AbstractGatewayModuleHook` and implements lifecycle methods (setup, startup, shutdown). It is where all module logic lives, including:
- Config panels (`getConfigPanels()`)
- Web routes (`mountRouteHandlers()`)
- Resource mounting (`getMountedResourceFolder()`)
- Licensing (`isFreeModule()`)

## Docker Development Environment

For local testing with Ignition 8.3.1, use the provided Docker Compose setup:

```bash
# Start Ignition and proxy
docker-compose up -d

# Access gateway
open http://localhost:8088
# Credentials: admin / password

# View logs
docker-compose logs -f
```

See **`docs/setup_environment.md`** for complete documentation including:
- Factry Historian setup
- Building and installing the module
- Running tests
- Troubleshooting

The `ignition/` folder contains persistent data and is git-ignored.

## Development Workflow

### Module Update Workflow

**IMPORTANT**: Browser-based module upload via the Ignition Gateway web interface does not work reliably.

**Use the Docker volume mounting workflow instead**:

1. Build the module: `./gradlew clean build`
2. Copy to Docker volume: `cp build/Factry-Historian.unsigned.modl ignition/data/modules/`
3. Restart Gateway: `docker-compose restart ignition`
4. On first install: Accept the **Factry** certificate in **Config → System → Modules**

See **`docs/gradlew_commands.md`** for build, test, and deploy commands.

### Adding Gateway Functionality

1. Implement logic in `gateway/src/main/java/io/factry/historian/gateway/`
2. Add Ignition SDK dependencies in `gateway/build.gradle.kts`:
   ```kotlin
   dependencies {
       compileOnly("com.inductiveautomation.ignitionsdk:gateway-api:${rootProject.extra["sdk_version"]}")
   }
   ```
3. Override relevant hook methods in `FactryHistorianGatewayHook`

### Adding Module Dependencies

**Use `moduleDependencySpecs` (Ignition 8.3+)** to declare module dependencies with the "required" flag:

```kotlin
moduleDependencySpecs {
    register("com.inductiveautomation.opcua") {
        scope = "G"           // Gateway scope only
        required = true       // Module won't load without this dependency
    }
    register("com.inductiveautomation.vision") {
        scope = "CD"          // Client and Designer scopes
        required = false      // Optional dependency
    }
}
```

**Do NOT use the legacy `moduleDependencies` map** - it's for Ignition 8.1/8.2 compatibility only:
```kotlin
// ❌ DON'T USE - Legacy pattern for 8.1/8.2
moduleDependencies.set(mapOf("G" to "com.inductiveautomation.opcua"))
```

The `moduleDependencySpecs` block allows marking dependencies as required, which causes the gateway to fault on module load if dependencies are missing (8.3+ feature).

### Gradle Plugin

This project uses the `io.ia.sdk.modl` Gradle plugin (v0.4.0) which:
- Packages all scopes into a single `.modl` file
- Signs the module with provided certificates
- Generates the `module.xml` descriptor
- Assembles dependencies into `build/moduleContent/`

## Repository URLs

The project uses Inductive Automation's Maven repository:
- **URL**: `https://nexus.inductiveautomation.com/repository/public`
- Configured in both `settings.gradle` (dependencies) and plugin management

## Documentation

Development documentation: `docs/content.md`

### Ignition SDK Resources (8.3+)
When implementing features, always reference Ignition 8.3+ documentation:
- **SDK JavaDocs**: https://docs.inductiveautomation.com/docs/8.3/appendix/javadocs
- **Module Development Guide**: Focus on 8.3+ sections
- **SDK Examples**: Look for examples specifically marked as 8.3 compatible
- **Forum Posts**: Filter for 8.3+ discussions when searching for solutions

## Common Issues

### Build fails with "cannot access class com.sun.tools.javac.main.JavaCompiler"
- **Cause**: Building with wrong Java version (not Java 17)
- **Solution**: Ensure Java 17 is installed and active: `java -version`

### "Required certificate file location not found"
- **Cause**: Missing or incorrectly configured `gradle.properties`
- **Solution**: Either configure certificates in `gradle.properties` or set `skipModlSigning.set(true)` in `build.gradle.kts`

### Module doesn't appear in Ignition after installation
- Check that hook classes are correctly mapped in the `hooks` block
- Verify module ID matches between `build.gradle.kts` and `FactryHistorianModule.MODULE_ID`
- Check Ignition gateway logs for loading errors
