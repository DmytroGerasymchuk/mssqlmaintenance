# Projekt-Beschreibung
Das Projekt realisiert in T-SQL-Code (und nicht nur) das Verfahrensframework "MSSQL*Maintenance" für Unterstützung von "Database Administration Lifecycle".

# Was ist "Database Administration Lifecycle"?
Darunter versteht man die Abschnitte, welche in einem Datenbank-System nacheinander oder z. T. parallel auftreten:


# Einrichtung
	* SQL Server, SQL Server Client
	* nach Best Practices / Richtlinien
# Wartung
	* Standardisierte automatische Wartung
	* **{"MSSQL**Maintenance"}** stellt Routinen für Standard-Aufgaben und automatische Wartungs-Jobs zur Verfügung
# Verwaltung
	* Umfassende Überwachung und Auswertung
	* Durch Management Data Warehouse (MDW), Traces (PerfMon und Extended Events), Alerts
	* **{"MSSQL**Maintenance"}** stellt zusätzliche MDW-Kollektoren, standarisierte Traces und wichtigste Alerts bereit
# Performance-Monitoring und -Optimierung
	* **{"MSSQL**Maintenance"}** stellt Routinen bereit, welche die Analyse der durch Dynamic Management Views und MDW verfügbaren Informationen vereinheitlichen und vereinfachen

# Ursprungsideen

Viele der Richtlinien und Best Practices, insbesondere für Installation und anschließende Wartung, wurden zuerst in dem Buch [SQL Server Architekturskizzen: Best Practices aus der Praxis](https://www.amazon.de/dp/B00NMEEFDY) beschrieben, und dann  "physisch" als Software in Rahmen dieses Projektes umgesetzt.

# Wie ist MSSQL*Maintenance realisiert?
Im Wesentlichen besteht das Framework aus einer kleinen Wartungs-Datenbank, mit Default-Namen **MaintDB**, welche in jeder Managed-Instanz des SQL Servers eingerichtet wird.

Die Einrichtung erfolgt automatisch durch einen interaktiven Installer in Form eines CMD-Skriptes. Bei der Installation wird sowohl die Datenbank eingerichtet als auch die folgenden Komponenten:
* Operator für Benachrichtigungen
* Standardisierte Alerts
* Automatische Wartungs-Jobs

Durch separate Ausführung weiterer Skripte bzw. Verfahren können dann eingerichtet werden:
* Zusätzliche MDW-Kollektoren
* Extended Events-Trace
* PerfMon-Traces für OS- und SQL Server-Performance-Counters