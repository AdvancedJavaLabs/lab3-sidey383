plugins {
    java
}

group = "org.itmo"
version = "1.0"

java {
    targetCompatibility = JavaVersion.VERSION_1_8
    sourceCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-client:3.4.2")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.2")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.2")

    compileOnly("org.projectlombok:lombok:1.18.42")
    annotationProcessor("org.projectlombok:lombok:1.18.42")
}

tasks.withType<Jar> {
    archiveBaseName.set("app")
}