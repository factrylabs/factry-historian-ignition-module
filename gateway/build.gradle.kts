import com.google.protobuf.gradle.*
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.gradle.kotlin.dsl.KotlinClosure2

plugins {
    `java-library`
    id("com.google.protobuf") version "0.9.4"
}

java {
    toolchain {
        languageVersion.set(org.gradle.jvm.toolchain.JavaLanguageVersion.of(17))
    }
}

val grpcVersion = "1.62.2"
val protobufVersion = "3.25.3"

dependencies {
    compileOnly("com.inductiveautomation.ignitionsdk:ignition-common:${rootProject.extra["sdk_version"]}")
    compileOnly("com.inductiveautomation.ignitionsdk:gateway-api:${rootProject.extra["sdk_version"]}")

    // Historian API dependencies
    // These are in separate artifacts because historian is now a dedicated module
    // The SDK POMs (com.inductiveautomation.ignitionsdk) reference the real artifacts (com.inductiveautomation.historian)
    // We need to add the real artifacts directly since compileOnly doesn't pull transitive dependencies
    compileOnly("com.inductiveautomation.historian:historian-gateway:1.3.3") {
        isTransitive = false
    }
    compileOnly("com.inductiveautomation.historian:historian-gateway-api:1.3.3") {
        isTransitive = false
    }
    compileOnly("com.inductiveautomation.historian:historian-common:1.3.3") {
        isTransitive = false
    }

    // JSON library for HTTP communication with proxy
    compileOnly("com.google.code.gson:gson:2.10.1")

    // gRPC and Protobuf dependencies (bundled in .modl via modlImplementation)
    modlImplementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    modlImplementation("io.grpc:grpc-protobuf:$grpcVersion")
    modlImplementation("io.grpc:grpc-stub:$grpcVersion")
    modlImplementation("com.google.protobuf:protobuf-java:$protobufVersion")
    modlImplementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    modlImplementation("javax.annotation:javax.annotation-api:1.3.2")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("com.inductiveautomation.ignitionsdk:ignition-common:${rootProject.extra["sdk_version"]}")
    testImplementation("com.inductiveautomation.historian:historian-gateway-api:1.3.3") {
        isTransitive = false
    }
    testImplementation("com.inductiveautomation.historian:historian-common:1.3.3") {
        isTransitive = false
    }
    testImplementation("com.google.protobuf:protobuf-java:$protobufVersion")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.12")
}

// ---------------------------------------------------------------------------
// Integration test source set
// ---------------------------------------------------------------------------
sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        // Reuse main's compiled classes (including generated proto stubs) and resources
        compileClasspath += sourceSets.main.get().output + configurations["modlImplementation"]
        runtimeClasspath += sourceSets.main.get().output + configurations["modlImplementation"]
    }
}

val integrationTestImplementation by configurations.getting {
    extendsFrom(configurations["implementation"], configurations["modlImplementation"])
}
val integrationTestRuntimeOnly by configurations.getting {
    extendsFrom(configurations["runtimeOnly"])
}

dependencies {
    integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    integrationTestImplementation("com.google.code.gson:gson:2.10.1")
    integrationTestRuntimeOnly("org.junit.platform:junit-platform-launcher")
    integrationTestRuntimeOnly("org.slf4j:slf4j-simple:2.0.12")
}

tasks.register<Test>("integrationTest") {
    group = "verification"
    description = "Run integration tests against running Ignition + Factry"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()

    // Always re-run — integration tests depend on external services, not just code
    outputs.upToDateWhen { false }

    // Resolve the Factry collector token, in priority order:
    //   1. COLLECTOR_TOKEN env var (explicit override)
    //   2. script/.collector-token file (durable local token — survives UI reconfig,
    //      which stores the historian token encrypted rather than in plaintext config)
    //   3. the historian config created by setup-historians.sh
    val historianConfigDir = file("../ignition/data/config/resources/core/com.inductiveautomation.historian/historian-provider")
    val tokenFromConfig = findTokenFromHistorianConfig(historianConfigDir)
    val tokenFile = rootProject.file("script/.collector-token")
    val tokenFromFile = if (tokenFile.exists()) tokenFile.readText().trim() else ""
    val resolvedToken = when {
        !System.getenv("COLLECTOR_TOKEN").isNullOrBlank() -> System.getenv("COLLECTOR_TOKEN")
        tokenFromFile.isNotBlank() -> tokenFromFile
        else -> tokenFromConfig
    }

    systemProperty("gateway.url", System.getenv("GATEWAY_URL") ?: "http://localhost:8089")
    systemProperty("webdev.project", System.getenv("WEBDEV_PROJECT") ?: "TestFactry")
    systemProperty("grpc.host", System.getenv("GRPC_HOST") ?: "localhost")
    systemProperty("grpc.port", System.getenv("GRPC_PORT") ?: "8001")
    systemProperty("collector.token", resolvedToken)
    systemProperty("gateway.system.name", System.getenv("GATEWAY_SYSTEM_NAME") ?: "Ignition-FactryTest")
    systemProperty("collector.name", System.getenv("COLLECTOR_NAME") ?: "Ignition")
    systemProperty("historian.name", System.getenv("HISTORIAN_NAME") ?: "Factry Historian")

    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
        afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ desc, result ->
            if (desc.parent == null) {
                println("\nTest results: ${result.resultType} " +
                    "(${result.testCount} tests, ${result.successfulTestCount} passed, " +
                    "${result.failedTestCount} failed, ${result.skippedTestCount} skipped)")
            }
        }))
    }
}

// Ensure integration test compilation sees main sources
tasks.named<JavaCompile>("compileIntegrationTestJava") {
    dependsOn(tasks.named("compileJava"))
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
        afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ desc, result ->
            if (desc.parent == null) {
                println("\nTest results: ${result.resultType} " +
                    "(${result.testCount} tests, ${result.successfulTestCount} passed, " +
                    "${result.failedTestCount} failed, ${result.skippedTestCount} skipped)")
            }
        }))
    }
}

// Ensure compileTestJava sees test sources (protobuf plugin can interfere)
tasks.named<JavaCompile>("compileTestJava") {
    dependsOn(tasks.named("compileJava"))
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

sourceSets {
    main {
        proto {
            srcDir("${rootProject.projectDir}/proto")
        }
    }
}

tasks.named<ProcessResources>("processResources") {
    val moduleVersion = rootProject.extensions
        .getByType<io.ia.sdk.gradle.modl.extension.ModuleSettings>()
        .moduleVersion
    inputs.property("moduleVersion", moduleVersion)
    filesMatching("version.properties") {
        expand("moduleVersion" to moduleVersion.get())
    }
}

/**
 * Reads the collector token from the first Factry historian config found in the Ignition data folder.
 * This avoids hardcoding tokens in the build file — the setup scripts create the config.
 */
fun findTokenFromHistorianConfig(configDir: File): String {
    if (!configDir.exists()) return ""
    configDir.listFiles()?.forEach { dir ->
        val configFile = File(dir, "config.json")
        if (configFile.exists()) {
            val content = configFile.readText()
            if (content.contains("\"factry-historian\"")) {
                val match = Regex("\"token\"\\s*:\\s*\"([^\"]+)\"").find(content)
                if (match != null) return match.groupValues[1]
            }
        }
    }
    return ""
}
