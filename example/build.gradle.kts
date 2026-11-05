plugins {
    id("buildlogic.java-application-conventions")
}

group = project.group
version = project.version

dependencies {
    implementation(project(":event-admin"))
    implementation("org.slf4j:slf4j-api:2.0.20")
    runtimeOnly("ch.qos.logback:logback-classic:1.6.6")
}

application {
    mainModule.set("event.bus.example")
    mainClass.set("av.kabanov.example.app.RestaurantWorkflowApp")
}