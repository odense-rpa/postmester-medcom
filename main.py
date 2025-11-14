import argparse
import asyncio
import logging
import sys
import os

from automation_server_client import AutomationServer, Workqueue, WorkItemError, Credential, WorkItemStatus
from datetime import datetime, timedelta
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.utils import (
    sanitize_cpr
)
from odk_tools.tracking import Tracker
from odk_tools.reporting import report
from process.config import load_excel_mapping, get_excel_mapping

nexus: NexusClientManager
tracker: Tracker

proces_navn = "Postmester Medcom"

def match_regel(regel: dict, data: dict) -> bool:        
    if regel.get("Emne") and str(data.get("name", "")).lower() == str(regel["Emne"]).lower():
        return True
    if regel.get("Wildcard søgning i emnefelt") == "Ja" and str(data.get("name", "")).lower().startswith(str(regel["Emne"]).lower()):
        return True

    return False

def tilføj_organisationer(borger: dict, regel: dict, data: dict):
    if any(obj["organization"]["name"] == regel.get("Organisation") and obj["effectiveEndDate"] is None for obj in data["patientOrganizations"]):
        return
    
    organisation = nexus.organisationer.hent_organisation_ved_navn(
        navn=str(regel.get("Organisation"))
    )

    if organisation is None:
        return

    nexus.organisationer.tilføj_borger_til_organisation(
        borger=borger,
        organisation=organisation
    )

    report(
        report_id="postmester_medcom",
        group="Udført af TYRA",
        json={
            "cpr": borger.get("patientIdentifier").get("identifier"),
            "emne": data.get("name", ""),
            "handling": f"Tilføjet organisation: {regel.get('Organisation')}"
        }
    )    

def tilføj_forløb(borger: dict, regel: dict, data: dict):
    borgers_forløb = nexus.borgere.hent_aktive_forløb(borger=borger)
    linjer = regel.get("Forløb")

    if linjer is None:
        return

    linjer = str(linjer).split("\n")

    for linje in linjer:
        if "/" in linje:            
            parts = linje.split("/", 1)
            grundforløb = parts[0].strip()
            forløb = parts[1].strip()

            # Check if grundforløb and forløb already exist in borgers_forløb
            if any(f["name"] == grundforløb for f in borgers_forløb) and any(f["name"] == forløb for f in borgers_forløb):
                continue

            nexus.forløb.opret_forløb(borger=borger, grundforløb_navn=grundforløb, forløb_navn=forløb)

            report(
                report_id="postmester_medcom",
                group="Udført af TYRA",
                json={
                    "cpr": borger.get("patientIdentifier").get("identifier"),
                    "emne": data.get("name", ""),
                    "handling": f"Tilføjet grundforløb: {grundforløb} og forløb: {forløb}"
                }
            )
        else:            
            grundforløb = linje.strip()

            if any(f["name"] == grundforløb for f in borgers_forløb):
                continue

            nexus.forløb.opret_forløb(borger=borger, grundforløb_navn=grundforløb)
    
            report(
                report_id="postmester_medcom",
                group="Udført af TYRA",
                json={
                    "cpr": borger.get("patientIdentifier").get("identifier"),
                    "emne": data.get("name", ""),
                    "handling": f"Tilføjet grundforløb: {grundforløb}"
                }
            )

def tilføj_opgaver(borger: dict, regel: dict, data: dict):
    if regel.get("Opgavetype") is None:
        return

    reference = data.get("_links").get("referencedObject").get("href")
    medcom_besked = nexus.nexus_client.get(endpoint=reference).json()
    opgaver_på_besked = nexus.opgaver.hent_opgaver(medcom_besked)

    for opgave in opgaver_på_besked:
        if (opgave["type"]["name"] == regel.get("Opgavetype") and opgave["organizationAssignee"]["displayName"].lower() == str(regel.get("Organisation")).lower()):
            return

    nexus.opgaver.opret_opgave(
        objekt=medcom_besked,
        opgave_type=str(regel.get("Opgavetype")),
        titel=str(regel.get("Opgavetype")),
        ansvarlig_organisation=str(regel.get("Organisation")),
        start_dato=datetime.now().date(),
        forfald_dato=datetime.now().date() + timedelta(days=1)
    )

    report(
        report_id="postmester_medcom",
        group="Udført af TYRA",
        json={
            "cpr": borger.get("patientIdentifier").get("identifier"),
            "emne": data.get("name", ""),
            "handling": f"Tilføjet opgave på besked med emne: {data.get('name', '')} til person: {borger.get("patientIdentifier").get("identifier")}"
        }
    )

async def populate_queue(workqueue: Workqueue):
    aktivitetsliste = nexus.aktivitetslister.hent_aktivitetsliste(
        navn="MedCom - Korrespondancer: venter + accepterede", 
        organisation=None,
        medarbejder=None,
        antal_sider=10
    )

    if aktivitetsliste:
        for aktivitet in aktivitetsliste:
            eksisterende_kødata = workqueue.get_item_by_reference(str(aktivitet["id"]))

            if len(eksisterende_kødata) > 0:
                continue

            workqueue.add_item(aktivitet, str(aktivitet["id"]))

async def process_workqueue(workqueue: Workqueue):    
    logger = logging.getLogger(__name__)
    regler = get_excel_mapping()

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict            
            cpr = data.get("patients")[0].get("patientIdentifier").get("identifier")
            cpr = sanitize_cpr(cpr=cpr)
            borger = nexus.borgere.hent_borger(borger_cpr=cpr)

            if borger is None:
                continue
 
            try:
                for regel in regler:
                    if match_regel(regel, data):                        
                        tilføj_organisationer(borger, regel, data)
                        tilføj_forløb(borger, regel, data)
                        tilføj_opgaver(borger, regel, data)
                        tracker.track_task(process_name=proces_navn)

            except WorkItemError as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {data}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO        
    )

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")    
    tracking_credential = Credential.get_credential("Odense SQL Server")

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )    
    
    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel-file",
        default="./Regler.xlsx",
        help="Path to the Excel file containing mapping data (default: ./Regler.xlsx)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate Excel file exists
    if not os.path.isfile(args.excel_file):
        raise FileNotFoundError(f"Excel file not found: {args.excel_file}")

    # Load excel mapping data once on startup
    load_excel_mapping(args.excel_file)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))