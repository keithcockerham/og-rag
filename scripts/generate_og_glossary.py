#!/usr/bin/env python3
"""
O&G Terminology Database Generator
Creates a comprehensive glossary from curated industry definitions
No scraping required - uses embedded expert knowledge
"""

from pathlib import Path
import json

OUTPUT_DIR = Path("data/raw/og_glossary")

# Curated O&G terminology with definitions
# Sources: SPE, API, IADC, industry standard references
OG_GLOSSARY = [
    # === Artificial Lift - ESP ===
    {
        "term": "Electric Submersible Pump (ESP)",
        "definition": "A multistage centrifugal pump driven by a downhole electric motor, used as an artificial lift method to produce fluids from wells. The ESP system consists of a motor, seal section (protector), pump intake, multistage centrifugal pump, and power cable. ESPs are efficient for high-volume production but sensitive to gas, solids, and temperature.",
        "category": "artificial_lift",
        "related": ["artificial lift", "centrifugal pump", "motor", "protector"]
    },
    {
        "term": "Gas Lock",
        "definition": "A condition where free gas accumulates in the pump stages, preventing the pump from moving fluid. In ESPs, gas lock occurs when gas volume exceeds the pump's ability to compress and move the gas-liquid mixture, causing the pump to lose prime and produce little or no fluid. Remedies include gas separators, gas handlers, and variable speed drives.",
        "category": "artificial_lift",
        "related": ["ESP", "gas separator", "pump off"]
    },
    {
        "term": "Pump Off",
        "definition": "A condition where fluid supply to the pump intake is insufficient to keep the pump fully loaded, causing the pump to operate in a gas or vapor state. This leads to overheating, mechanical damage, and reduced run life. Pump-off controllers monitor motor load or other parameters to shut down or slow the pump when this condition is detected.",
        "category": "artificial_lift",
        "related": ["ESP", "pump off controller", "fluid level"]
    },
    {
        "term": "Motor Temperature",
        "definition": "The operating temperature of a downhole ESP motor. Motors are cooled by the flow of produced fluid past the motor housing. High motor temperature can result from insufficient fluid flow, high bottomhole temperature, or electrical issues. Most ESP motors are rated for 250-300°F, with high-temperature versions rated to 400°F or higher.",
        "category": "artificial_lift",
        "related": ["ESP", "motor", "cooling"]
    },
    {
        "term": "Protector (Seal Section)",
        "definition": "The component between the ESP motor and pump that provides a seal between motor oil and well fluids, equalizes pressure, and accommodates thermal expansion of motor oil. Contains thrust bearings to handle axial loads from the pump. Protector failure is a common cause of ESP system failure.",
        "category": "artificial_lift",
        "related": ["ESP", "motor", "thrust bearing"]
    },
    {
        "term": "Variable Speed Drive (VSD)",
        "definition": "A surface controller that varies the frequency and voltage supplied to an ESP motor, allowing adjustment of pump speed. VSDs enable optimization of production rate, reduce power consumption, provide soft start capability, and help manage gas and changing well conditions. Also called Variable Frequency Drive (VFD).",
        "category": "artificial_lift",
        "related": ["ESP", "motor", "frequency"]
    },
    {
        "term": "Gas Separator",
        "definition": "A downhole device installed below the ESP pump intake that separates free gas from the produced fluid stream before it enters the pump. Works by using gravity, centrifugal force, or both to divert gas up the casing annulus while liquid enters the pump. Critical for wells with high gas-to-liquid ratios.",
        "category": "artificial_lift",
        "related": ["ESP", "gas lock", "GOR"]
    },
    # === Well Control ===
    {
        "term": "Kick",
        "definition": "An influx of formation fluids (gas, oil, or water) into the wellbore during drilling operations when formation pressure exceeds the hydrostatic pressure of the drilling fluid. Early kick detection and proper response are critical to prevent blowouts. Warning signs include pit gain, increased flow rate, and drilling breaks.",
        "category": "well_control",
        "related": ["blowout", "mud weight", "shut-in"]
    },
    {
        "term": "Blowout",
        "definition": "An uncontrolled flow of formation fluids from the wellbore to surface or into another formation. Blowouts occur when well control is lost and the BOP system fails to contain the kick. Can result in loss of life, environmental damage, and loss of the well. Prevention through proper well control practices is paramount.",
        "category": "well_control",
        "related": ["kick", "BOP", "well control"]
    },
    {
        "term": "Blowout Preventer (BOP)",
        "definition": "A large valve or system of valves installed at the wellhead to seal, control, and monitor the well. BOPs can close around the drill pipe (pipe rams), seal an open hole (blind rams), or cut through drill pipe and seal (shear rams). Annular preventers can close on any size pipe or open hole. BOPs are the primary line of defense against blowouts.",
        "category": "well_control",
        "related": ["rams", "annular preventer", "well control"]
    },
    {
        "term": "Shut-In Drill Pipe Pressure (SIDPP)",
        "definition": "The pressure observed on the drill pipe after shutting in a well following a kick. SIDPP represents the difference between formation pressure and the hydrostatic pressure of the mud column in the drill pipe. Used to calculate kill mud weight. SIDPP = Formation Pressure - Hydrostatic Pressure of mud.",
        "category": "well_control",
        "related": ["kick", "kill weight", "SICP"]
    },
    {
        "term": "Shut-In Casing Pressure (SICP)",
        "definition": "The pressure observed on the annulus (casing) side after shutting in a well following a kick. SICP is influenced by the type and volume of influx in the annulus. SICP is typically higher than SIDPP when gas is present due to the lower density of the gas column.",
        "category": "well_control",
        "related": ["kick", "SIDPP", "annulus"]
    },
    {
        "term": "Kill Weight Mud",
        "definition": "The mud weight required to balance formation pressure and stop an influx. Calculated as: Kill Weight = Original Mud Weight + (SIDPP / (0.052 × True Vertical Depth)). The well is circulated with kill weight mud to regain primary well control.",
        "category": "well_control",
        "related": ["kick", "SIDPP", "mud weight"]
    },
    {
        "term": "Driller's Method",
        "definition": "A well kill procedure where the kick is first circulated out with the original mud weight, then kill weight mud is circulated. Requires two complete circulations. Simpler but results in higher pressures during the kill operation compared to Wait and Weight method.",
        "category": "well_control",
        "related": ["well kill", "Wait and Weight", "kick"]
    },
    {
        "term": "Wait and Weight Method",
        "definition": "A well kill procedure (also called Engineer's Method) where kill weight mud is prepared before circulation begins, then the kick is displaced in a single circulation with the heavier mud. Results in lower pressures than Driller's Method but requires time to weight up the mud.",
        "category": "well_control",
        "related": ["well kill", "Driller's Method", "kick"]
    },
    # === Drilling Operations ===
    {
        "term": "Mud Weight",
        "definition": "The density of drilling fluid, typically expressed in pounds per gallon (ppg) or specific gravity. Mud weight must be sufficient to maintain hydrostatic pressure greater than formation pore pressure (to prevent kicks) but less than formation fracture pressure (to prevent losses). Also called mud density.",
        "category": "drilling",
        "related": ["hydrostatic pressure", "kick", "lost circulation"]
    },
    {
        "term": "Lost Circulation",
        "definition": "The loss of drilling fluid to the formation through fractures, vugs, or high-permeability zones. Losses can be partial or total. Treatment includes lost circulation material (LCM), cement squeezes, or reducing mud weight. Severe losses can lead to well control problems if fluid level drops.",
        "category": "drilling",
        "related": ["mud weight", "LCM", "fracture"]
    },
    {
        "term": "Rate of Penetration (ROP)",
        "definition": "The speed at which the drill bit advances through the formation, typically measured in feet per hour (ft/hr). ROP is influenced by weight on bit, rotary speed, bit type, formation hardness, and hydraulics. A sudden increase in ROP (drilling break) may indicate drilling into a higher-pressure zone.",
        "category": "drilling",
        "related": ["drilling break", "WOB", "bit"]
    },
    {
        "term": "Wellbore Stability",
        "definition": "The ability of the wellbore wall to maintain its structural integrity during and after drilling. Instability can cause hole collapse, tight hole, stuck pipe, or wellbore enlargement. Factors include mud weight, mud chemistry, formation properties, wellbore trajectory, and time.",
        "category": "drilling",
        "related": ["mud weight", "shale", "stuck pipe"]
    },
    # === Production Operations ===
    {
        "term": "Wellhead",
        "definition": "The equipment installed at the surface of a well to provide structural support for casing strings, pressure containment, and a connection point for surface flow control equipment. Includes casing heads, tubing heads, and the Christmas tree. Rated for specific pressure and temperature.",
        "category": "production",
        "related": ["Christmas tree", "casing", "tubing"]
    },
    {
        "term": "Christmas Tree",
        "definition": "The assembly of valves, fittings, and connections installed on top of the wellhead to control flow from the well. Includes master valves, wing valves, choke, and pressure gauges. Allows well to be shut in, flow controlled, and provides access for wireline operations. Named for its branching appearance.",
        "category": "production",
        "related": ["wellhead", "choke", "master valve"]
    },
    {
        "term": "Separator",
        "definition": "A vessel used to separate produced well fluids into gas, oil, and water phases. Works by reducing fluid velocity and using gravity, baffles, and residence time. Types include two-phase (gas-liquid), three-phase (gas-oil-water), horizontal, vertical, and spherical separators.",
        "category": "production",
        "related": ["production", "gas", "oil", "water"]
    },
    {
        "term": "Choke",
        "definition": "A restriction device used to control flow rate and reduce pressure. Surface chokes are part of the Christmas tree or choke manifold. Adjustable chokes allow flow control; positive chokes have fixed orifice size. Critical for managing well flow and preventing equipment damage from excessive rates.",
        "category": "production",
        "related": ["Christmas tree", "flow rate", "pressure"]
    },
    # === Safety & Hazards ===
    {
        "term": "Hydrogen Sulfide (H2S)",
        "definition": "A highly toxic, flammable gas with a characteristic rotten egg odor at low concentrations. Colorless and heavier than air. Extremely dangerous - can cause rapid unconsciousness (knockdown) and death at concentrations above 100 ppm. OSHA PEL is 20 ppm ceiling. Common in sour oil and gas operations. Requires continuous monitoring and respiratory protection.",
        "category": "safety",
        "related": ["sour gas", "PPE", "SCBA"]
    },
    {
        "term": "Confined Space",
        "definition": "An enclosed space with limited entry/exit that is not designed for continuous occupancy. In oil and gas operations includes tanks, vessels, pits, and cellars. Hazards include oxygen deficiency, toxic atmospheres, engulfment, and entrapment. Requires permits, atmospheric testing, ventilation, and rescue plans before entry.",
        "category": "safety",
        "related": ["H2S", "permit", "atmospheric testing"]
    },
    {
        "term": "Process Safety Management (PSM)",
        "definition": "A systematic approach to managing hazards associated with processes using highly hazardous chemicals. OSHA 29 CFR 1910.119 requires PSM for facilities with threshold quantities of hazardous materials. Elements include process hazard analysis, operating procedures, training, mechanical integrity, and emergency response.",
        "category": "safety",
        "related": ["OSHA", "hazard analysis", "MOC"]
    },
    # === Completions ===
    {
        "term": "Perforation",
        "definition": "Holes made through casing, cement, and into the formation to establish communication between the wellbore and reservoir. Created by shaped explosive charges (perforating guns) or abrasive jetting. Perforation parameters (size, density, phasing, penetration) affect well productivity.",
        "category": "completions",
        "related": ["casing", "formation", "productivity"]
    },
    {
        "term": "Hydraulic Fracturing",
        "definition": "A stimulation technique where fluid is pumped at high pressure to create fractures in the formation, improving flow paths to the wellbore. Proppant (sand or ceramic beads) holds fractures open after pumping stops. Critical for low-permeability reservoirs including shale. Also called fracking or frac job.",
        "category": "completions",
        "related": ["stimulation", "proppant", "shale"]
    },
    {
        "term": "Sand Control",
        "definition": "Methods to prevent formation sand from entering the wellbore and damaging equipment or reducing production. Techniques include gravel packs, frac packs, screens, and chemical consolidation. Especially important in unconsolidated formations with fine-grained sand.",
        "category": "completions",
        "related": ["gravel pack", "screen", "formation"]
    },
    # === Flow Assurance ===
    {
        "term": "Scale",
        "definition": "Mineral deposits (typically calcium carbonate, barium sulfate, or calcium sulfate) that precipitate from produced water and accumulate in tubing, equipment, and near-wellbore area. Reduces flow capacity and can damage equipment. Treated with scale inhibitors or mechanical/chemical removal.",
        "category": "flow_assurance",
        "related": ["produced water", "inhibitor", "precipitation"]
    },
    {
        "term": "Wax (Paraffin)",
        "definition": "Heavy hydrocarbon compounds that precipitate from crude oil as temperature decreases below the wax appearance temperature (WAT). Deposits restrict flow in tubing and flowlines. Managed through insulation, heating, chemical inhibitors, pigging, or hot oiling treatments.",
        "category": "flow_assurance",
        "related": ["crude oil", "temperature", "pigging"]
    },
    {
        "term": "Hydrates",
        "definition": "Ice-like crystalline compounds formed when water molecules cage natural gas molecules (primarily methane) at high pressure and low temperature. Can plug flowlines and equipment. Prevented by dehydration, chemical inhibitors (methanol, glycol), insulation, or heating.",
        "category": "flow_assurance",
        "related": ["gas", "water", "methanol"]
    },
    {
        "term": "Corrosion",
        "definition": "The deterioration of metal through chemical or electrochemical reaction with the environment. In oil and gas, caused by CO2 (sweet corrosion), H2S (sour corrosion), oxygen, and chlorides. Managed through material selection, coatings, cathodic protection, and chemical inhibitors.",
        "category": "flow_assurance",
        "related": ["H2S", "CO2", "inhibitor"]
    },
    # === Reservoir ===
    {
        "term": "Porosity",
        "definition": "The percentage of rock volume that is void space capable of holding fluids. Types include primary (original depositional) and secondary (fractures, dissolution). Effective porosity counts only interconnected pores. Typical reservoir porosity ranges from 5% to 30%.",
        "category": "reservoir",
        "related": ["permeability", "formation", "saturation"]
    },
    {
        "term": "Permeability",
        "definition": "A measure of a rock's ability to transmit fluids, measured in darcies (D) or millidarcies (mD). Depends on pore size, pore throat geometry, and connectivity. Absolute permeability is measured with single-phase flow; effective and relative permeability apply to multiphase flow.",
        "category": "reservoir",
        "related": ["porosity", "formation", "Darcy"]
    },
    {
        "term": "Water Cut",
        "definition": "The percentage of water in total produced liquids, expressed as volume percent. Water cut typically increases over the life of a well as reservoir pressure depletes or water injection advances. High water cut increases lifting costs and water handling requirements.",
        "category": "reservoir",
        "related": ["production", "waterflood", "ESP"]
    },
    {
        "term": "Gas-Oil Ratio (GOR)",
        "definition": "The ratio of gas produced to oil produced, typically expressed in standard cubic feet per stock tank barrel (scf/STB). Initial GOR depends on reservoir conditions; increasing GOR during production may indicate gas cap expansion or solution gas drive depletion.",
        "category": "reservoir",
        "related": ["production", "solution gas", "gas cap"]
    },
]


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("O&G Terminology Database Generator")
    print("=" * 60)
    print(f"Terms in database: {len(OG_GLOSSARY)}")
    
    # Save as JSONL for processing
    jsonl_file = OUTPUT_DIR / "og_glossary.jsonl"
    with open(jsonl_file, 'w', encoding='utf-8') as f:
        for entry in OG_GLOSSARY:
            f.write(json.dumps(entry) + '\n')
    
    # Save as readable text file
    text_file = OUTPUT_DIR / "og_glossary.txt"
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write("# Oil & Gas Technical Glossary\n\n")
        f.write("Curated definitions from SPE, API, IADC, and industry references\n\n")
        f.write("=" * 60 + "\n\n")
        
        # Group by category
        categories = {}
        for entry in OG_GLOSSARY:
            cat = entry['category']
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(entry)
        
        for cat in sorted(categories.keys()):
            f.write(f"## {cat.upper().replace('_', ' ')}\n\n")
            
            for entry in sorted(categories[cat], key=lambda x: x['term']):
                f.write(f"### {entry['term']}\n\n")
                f.write(f"{entry['definition']}\n\n")
                if entry.get('related'):
                    f.write(f"*Related: {', '.join(entry['related'])}*\n\n")
                f.write("-" * 40 + "\n\n")
    
    # Summary
    print(f"\nSaved {len(OG_GLOSSARY)} terms to:")
    print(f"  {jsonl_file}")
    print(f"  {text_file}")
    
    print("\nTerms by category:")
    categories = {}
    for entry in OG_GLOSSARY:
        cat = entry['category']
        categories[cat] = categories.get(cat, 0) + 1
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")


if __name__ == "__main__":
    main()
