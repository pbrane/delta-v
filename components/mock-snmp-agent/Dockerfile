FROM eclipse-temurin:21-jre
COPY mock-snmp-agent.jar /app/mock-snmp-agent.jar
COPY snmpTestData1.properties /app/snmpTestData1.properties
EXPOSE 161/udp
ENTRYPOINT ["java", "-jar", "/app/mock-snmp-agent.jar", \
            "-d", "/app/snmpTestData1.properties", \
            "-l", "0.0.0.0", "-p", "161"]
