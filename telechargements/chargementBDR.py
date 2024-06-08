"""
Bienvenue sur le script Python qui permet le chargement automatique des données dans la base sdis79. 

Les méthodes utilisées sont les suivantes : 
- Si les fichiers CSV ont un format qui correspond aux tables dans la base, ils sont chargés directement.
- Si besoin, ils sont réarrangés pour correspondre au format requis.
- Certaines tables nécessitent de d'abord faire des jointures entre plusieurs fichiers CSV. Ces jointures sont faites dans les programmes en python qui crééent des nouveaux fichiers CSV.
"""
# /Users/remipierron/Library/Mobile Documents/com~apple~CloudDocs/SAE BDR/ Ceci permet d'utiliser les quatre lignes suivantes.

probleme = input("Rencontrez vous des problèmes liés aux chemins des fichiers ? (o/n)")
if probleme == 'o':
    chemin = input("Afin de résoudre le problème, veuillez copier-coller le chemin pour accéder aux fichiers ici : ")
else:
    chemin = ''
"""
import des librairies
"""
import csv
import mysql.connector

"""
Définition des paramètres de connexion à la base de donnée
"""
params = {"host": "localhost", "user": "root", "password": "", "database": "sdis79"}

"""
Remplissage de la table caserne
"""
try:
    with open(chemin+"caserne.csv", encoding='iso-8859-1') as affectation:
        contenu_csv = csv.reader(affectation, delimiter=';')
        next(contenu_csv)
        for row in contenu_csv:
            code, caserne = row
            
            requete = "INSERT INTO `caserne`(`codeCaserne`, `libCaserne`) VALUES (%s, %s)"
            valeurs = (code, caserne)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL1:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table grade
"""

try:
    with open(chemin+"grade.csv", encoding='iso-8859-1') as affectation:
        contenu_csv = csv.reader(affectation, delimiter=';')
        next(contenu_csv)
        for row in contenu_csv:
            codeGrade, libGrade = row
            requete = "INSERT INTO `grade`(`codeGrade`, `libGrade`) VALUES (%s, %s)"
            valeurs = (codeGrade, libGrade)

            try:
                with mysql.connector.connect(**params) as db:
                    # Création d'un curseur
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL2:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table employeur
"""
"""
Correction des numéros de téléphone
"""
with open(chemin+'volontaire.csv', 'r', encoding='iso-8859-1') as f:
    reader = csv.reader(f, delimiter=';')
    header = next(reader)

    with open(chemin+'volontaire2.csv', 'w', newline='', encoding='iso-8859-1') as g:
        writer = csv.writer(g, delimiter=';')
        writer.writerow(header)  

        for row in reader:
            tel = row[8]
            tel = '0' + tel
            if len(tel) < 10:
                tel += '0'
            row[8] = tel

            writer.writerow(row)
"""
Le terminal renvoie des erreurs de doublons de doublons qui sont normale car il ne peut pas y avoir deux employeurs identiques.
"""

try:
    with open(chemin+"volontaire2.csv", encoding='iso-8859-1') as affectation:
        contenu_csv = csv.reader(affectation, delimiter=';')
        next(contenu_csv)
        for row in contenu_csv:
            if len(row) >= 6:
                rue, cp, ville, tel, mail = row[-5:] 
                requete = "INSERT INTO `employeur`(`rue`, `cp`, `ville`, `tel`, `email`) VALUES (%s, %s, %s, %s, %s)"
                values = (rue, cp, ville, tel, mail)
    
                try:
                    with mysql.connector.connect(**params) as db:
                        with db.cursor() as c:
                            c.execute(requete, values)
                            db.commit()
                except mysql.connector.Error as err:
                    print("Erreur MySQL:", err)
            else:
                print("La ligne n'a pas le bon format:", row)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Création d'un employeur fantôme qui sert aux pompiers pro pour gérer les erreurs liées aux contraintes de clés étrangères
"""

try:
    requete = "INSERT INTO `employeur`(`rue`, `cp`, `ville`, `tel`, `email`) VALUES (%s, %s, %s, %s, %s)"
    values = ('00', '00', '00', '00', '00')
    try:
        with mysql.connector.connect(**params) as db:
            with db.cursor() as c:
                c.execute(requete, values)
                db.commit()
    except mysql.connector.Error as err:
        print("Erreur MySQL:", err) 
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table typeengin
"""

with open(chemin+"typeengin.csv", encoding='iso-8859-1') as affectation:
    contenu_csv = csv.reader(affectation, delimiter=';')

    next(contenu_csv)

    for row in contenu_csv:
        codeTypeEngin, libTypeEngin = row
        requete = "INSERT INTO `typeengin`(`codeTypeEngin`, `libTypeEngin`) VALUES ('" +codeTypeEngin+"','"+libTypeEngin+"')"

        try:
            with mysql.connector.connect(**params) as db:
                with db.cursor() as c:
                    c.execute(requete)
                    db.commit()
        except mysql.connector.Error as err:
            print("Erreur MySQL4:", err)

"""
Remplissage de la table Situation
"""

with open(chemin+"situation.csv", encoding='iso-8859-1') as affectation:
    contenu_csv = csv.reader(affectation, delimiter=';')
    for row in contenu_csv:
        if len(row) >= 2:
            refSituation, libSituation = row[:2]
            libSituation = libSituation.replace("'", " ")
            requete = "INSERT INTO `Situation`(`refSituation`, `libSituation`) VALUES (%s, %s)"

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, (refSituation, libSituation))
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL6:", err)

"""
Remplissage de la table Fonction
"""

try:
    with open(chemin+"fonction.csv", encoding='iso-8859-1') as affectation:
        contenu_csv = csv.reader(affectation, delimiter=';')
        next(contenu_csv)
        for row in contenu_csv:
            if len(row) >= 2:
                codeFonction, libFonction = row[:2] 
                libFonction = libFonction.replace("'", " ")
                
                requete = "INSERT INTO `Fonction`(`codeFonction`, `libFonction`) VALUES (%s, %s)"
                valeurs = (codeFonction, libFonction)

                try:
                    with mysql.connector.connect(**params) as db:
                        with db.cursor() as c:
                            c.execute(requete, valeurs)
                            db.commit()
                except mysql.connector.Error as err:
                    print("Erreur MySQL7:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table personnel
"""
"""
Correction du sexe
"""

with open(chemin+'pompier.csv', 'r', encoding='iso-8859-1') as f:
    reader = csv.reader(f, delimiter=';')

    with open(chemin+'pompier2.csv', 'w', newline='', encoding='iso-8859-1') as g:
        writer = csv.writer(g, delimiter=';')

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            if row[4].startswith('m'):
                row[4] = 'masculin'
            elif row[4].startswith('f'):
                row[4] = 'féminin'

            writer.writerow(row)

"""
Correction des numéros de tel des pompiers
"""
with open(chemin+'pompier2.csv', 'r', encoding='iso-8859-1') as f:
    reader = csv.reader(f, delimiter=';')
    header = next(reader)

    with open(chemin+'pompier3.csv', 'w', newline='', encoding='iso-8859-1') as g:
        writer = csv.writer(g, delimiter=';')
        writer.writerow(header) 

        for row in reader:
            tel = row[5]
            tel = '0' + tel
            if len(tel) < 10:
                tel += '0'
            row[5] = tel
            writer.writerow(row)


"""
1) Concaténation avec le code du grade
"""
code_grade = {}
with open(chemin+"grade.csv", encoding='iso-8859-1') as grade_file:
    grade_csv = csv.reader(grade_file, delimiter=';')
    next(grade_csv)
    for row in grade_csv:
        code_grade[row[1]] = row[0] 

concatenated_rows = []
with open(chemin+"pompier3.csv", encoding='iso-8859-1') as pompier_file:
    pompier_csv = csv.reader(pompier_file, delimiter=';')
    header = next(pompier_csv)
    for row in pompier_csv:
        lib_grade = row[-1]
        code = code_grade.get(lib_grade)
        if code:
            row.append(code)
            concatenated_rows.append(row)

with open(chemin+"personnel1.csv", mode='w', newline='', encoding='iso-8859-1') as concat_file:
    writer = csv.writer(concat_file, delimiter=';')
    writer.writerow(header + ["codeGrade"])  
    writer.writerows(concatenated_rows)


with open(chemin+"personnel1b.csv", mode='w', newline='', encoding='iso-8859-1') as personnel1b_file:
    writer = csv.writer(personnel1b_file, delimiter=';')
    new_header = [header[i] for i in range(len(header)) if i != header.index("grade")]  
    writer.writerow(new_header + ["codeGrade"]) 
    for row in concatenated_rows:
        new_row = [row[i] for i in range(len(row)) if i != header.index("grade")] 
        writer.writerow(new_row)

"""
2) concaténation avec empTel
"""

personnel1b_data = []
with open(chemin+"personnel1b.csv", encoding='iso-8859-1') as personnel1b_file:
    personnel1b_csv = csv.reader(personnel1b_file, delimiter=';')
    header = next(personnel1b_csv)
    for row in personnel1b_csv:
        personnel1b_data.append(row)

tel_by_matricule = {}
with open(chemin+"volontaire2.csv", encoding='iso-8859-1') as volontaire_file:
    volontaire_csv = csv.reader(volontaire_file, delimiter=';')
    next(volontaire_csv) 
    for row in volontaire_csv:
        matricule = row[0] 
        tel = row[-2] 
        tel_by_matricule[matricule] = tel 

with open(chemin+"personnel2.csv", mode='w', newline='', encoding='iso-8859-1') as personnel2_file:
    writer = csv.writer(personnel2_file, delimiter=';')
    writer.writerow(header[:-1] + ['codeGrade', 'empTel']) 
    for row in personnel1b_data:
        matricule = row[0] 
        tel = tel_by_matricule.get(matricule, "")  
        tel = '00' if tel == "" else tel  
        writer.writerow(row[:-1] + [row[-1], tel])  


"""
3) Suppression des doublons numBip et création de nouveau numBip
"""

personnel2_data = []
with open(chemin+"personnel2.csv", encoding='iso-8859-1') as personnel2_file:
    personnel2_csv = csv.reader(personnel2_file, delimiter=';')
    header = next(personnel2_csv) 
    for row in personnel2_csv:
        personnel2_data.append(row)


ensemble_num_bip = set()
doublons = set()
for row in personnel2_data:
    num_bip = int(row[6])  
    if num_bip in ensemble_num_bip:
        doublons.add(num_bip)
    else:
        ensemble_num_bip.add(num_bip)

for row in personnel2_data:
    num_bip = int(row[6]) 
    if num_bip in doublons:
        nouveau_num_bip = max(ensemble_num_bip) + 1
        ensemble_num_bip.add(nouveau_num_bip)
        row[6] = str(nouveau_num_bip) 

with open(chemin+"personnel3.csv", mode='w', newline='', encoding='iso-8859-1') as personnel3_file:
    writer = csv.writer(personnel3_file, delimiter=';')
    writer.writerow(header)  
    writer.writerows(personnel2_data) 
"""
4) Intervertir les colonnes empTel et codeGrade
"""

personnel3_data = []
with open(chemin+"personnel3.csv", encoding='iso-8859-1') as personnel3_file:
    personnel3_csv = csv.reader(personnel3_file, delimiter=';')
    for row in personnel3_csv:
        personnel3_data.append(row)

for row in personnel3_data:
    row[-1], row[-2] = row[-2], row[-1]

with open(chemin+"personnel4.csv", mode='w', newline='', encoding='iso-8859-1') as personnel4_file:
    writer = csv.writer(personnel4_file, delimiter=';')
    writer.writerows(personnel3_data)

"""
5) Envoi dans la Base sdis79
"""

try:
    with open(chemin+"personnel4.csv", encoding='iso-8859-1') as personnel_file:
        contenu_csv = csv.reader(personnel_file, delimiter=';')
        next(contenu_csv)  
        with mysql.connector.connect(**params) as db:
            with db.cursor() as c:
                for row in contenu_csv:
                    matriculePers, nom, prenom, dateNaissance, sexe, telephone, numBIP, dateEmbauche, dernierIndice, empTel, codeGrade = row
                    
                    if empTel is None:
                        empTel = "00"  
                    
                    requete = "INSERT INTO `personnel`(`matriculePers`, `nom`, `prenom`, `sexe`, `telephone`, `numBIP`, `dateEmbauche`, `idcTrait`, `empTel`, `codeGrade`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    
                    valeurs = (matriculePers, nom, prenom, sexe, telephone, numBIP, dateEmbauche, dernierIndice, empTel, codeGrade)

                    try:
                        c.execute(requete, valeurs)
                        db.commit()
                        
                    except mysql.connector.Error as err:
                        print("Erreur MySQL :", err)

except FileNotFoundError:
    print("Le fichier personnel4.csv n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite :", e)


"""
Remplissage de la table correspond
"""


try:
    with open(chemin+"situation.csv", encoding='iso-8859-1') as situation_file:
        contenu_csv = csv.reader(situation_file, delimiter=';')
        next(contenu_csv) 
        for row in contenu_csv:
            refSituation, libSituation, engin1, engin2, engin3 = row

            if engin1 and engin1.strip():
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        try:
                            requete = "INSERT INTO correspond (codeTypeEngin, refSituation) VALUES (%s, %s)"
                            c.execute(requete, (engin1, refSituation))
                            db.commit()
                        except mysql.connector.Error as err:
                            print("Erreur MySQL:", err)
            if engin2 and engin2.strip():
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        try:
                            requete = "INSERT INTO correspond (codeTypeEngin, refSituation) VALUES (%s, %s)"
                            c.execute(requete, (engin2, refSituation))
                            db.commit()
                        except mysql.connector.Error as err:
                            print("Erreur MySQL:", err)
            if engin3 and engin3.strip():
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        try:
                            requete = "INSERT INTO correspond (codeTypeEngin, refSituation) VALUES (%s, %s)"
                            c.execute(requete, (engin3, refSituation))
                            db.commit()
                        except mysql.connector.Error as err:
                            print("Erreur MySQL:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table habilitation
"""

try:
    with open(chemin+"fonction.csv", encoding='iso-8859-1') as fonction_file:
        contenu_csv = csv.reader(fonction_file, delimiter=';')
        next(contenu_csv) 
        for row in contenu_csv:
            codeFonction, libFonction = row
            
            requete = "INSERT INTO `Habilitation` (`codeHab`, `libelleHab`) VALUES (%s, %s)"
            valeurs = (codeFonction, libFonction)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table estHabilité
"""

"""
Extraction de la dernière habilitation et de la date correspondante
"""
pompiers = {}

with open(chemin+"habilitation.csv", newline='', encoding='iso-8859-1') as habilitation_file:
    contenu_csv = csv.reader(habilitation_file, delimiter=';')
    next(contenu_csv)
    for row in contenu_csv:
        matricule, _, _, *habilitations = row
        habilitations = [habilitation.strip() for habilitation in habilitations if habilitation.strip()]

        if matricule not in pompiers:
            pompiers[matricule] = []
        
        pompiers[matricule].extend(habilitations)

dernieres_habilitations = {}

for matricule, habilitations in pompiers.items():
    if habilitations:
        derniere_habilitation = habilitations[-2] if len(habilitations) >= 2 else habilitations[0]
        derniere_date = habilitations[-1] if len(habilitations) >= 2 else None
        dernieres_habilitations[matricule] = (derniere_habilitation, derniere_date)

with open(chemin+"habilitation2.csv", 'w', newline='', encoding='iso-8859-1') as sorted_habilitation_file:
    writer = csv.writer(sorted_habilitation_file, delimiter=';')
    writer.writerow(["matricule", "derniere_habilitation", "derniere_date"])
    for matricule, (habilitation, date) in dernieres_habilitations.items():
        writer.writerow([matricule, habilitation, date])


try:
    with open(chemin+"habilitation2.csv", encoding='iso-8859-1') as habilitation_trie_file:
        contenu_csv = csv.reader(habilitation_trie_file, delimiter=';')
        next(contenu_csv)
        for row in contenu_csv:
            matricule, codeHab, dateHab = row
            
            requete = "INSERT INTO `estHabilite` (`matriculePers`, `codeHab`, `dateHab`) VALUES (%s, %s, %s)"
            valeurs = (matricule, codeHab, dateHab)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table estAffecte
"""

"""
Ce script Python traite les données d'affectation des pompiers dans différentes casernes à partir d'un fichier CSV, ajoute le code correspondant à chaque caserne, 
concatène les données et écrit le résultat dans un nouveau fichier CSV. 
Il échange également les positions des colonnes "caserne" et "codeCaserne" dans le fichier de sortie final.
"""

input_file = chemin+"affectation.csv"
output_file = chemin+"affectation2.csv"

affectations = []

with open(input_file, newline='', encoding='iso-8859-1') as affectation_file:
    contenu_csv = csv.reader(affectation_file, delimiter=';')
    next(contenu_csv)  
    for row in contenu_csv:
        matricule, _, _, dateaffectation, caserne = row[:5]  
        affectations.append([matricule, dateaffectation, caserne])

with open(output_file, 'w', newline='', encoding='iso-8859-1') as sorted_affectation_file:
    writer = csv.writer(sorted_affectation_file, delimiter=';')
    writer.writerow(["matricule", "dateaffectation", "caserne"]) 
    writer.writerows(affectations) 

code_caserne = {}
with open(chemin+"caserne.csv", encoding='iso-8859-1') as caserne_file:
    caserne_csv = csv.reader(caserne_file, delimiter=';')
    next(caserne_csv) 
    for row in caserne_csv:
        code_caserne[row[1]] = row[0] 

concatenated_rows = []
with open(chemin+"affectation2.csv", encoding='iso-8859-1') as affectation_file:
    affectation_csv = csv.reader(affectation_file, delimiter=';')
    header = next(affectation_csv) 
    for row in affectation_csv:
        lib_caserne = row[-1]
        code = code_caserne.get(lib_caserne)
        if code:
            row.append(code)
            concatenated_rows.append(row)

with open(chemin+"affectation_caserne_concat_sans_caserne.csv", mode='w', newline='', encoding='iso-8859-1') as concat_file:
    writer = csv.writer(concat_file, delimiter=';')
    writer.writerow(header[:-1] + ["codeCaserne"])
    for row in concatenated_rows:
        writer.writerow(row[:-2] + [row[-1]]) 

with open(chemin+"affectation_caserne_concat_sans_caserne.csv", encoding='iso-8859-1') as input_file:
    reader = csv.reader(input_file, delimiter=';')
    header = next(reader)

    header[-1], header[-2] = header[-2], header[-1]

    rows = []
    for row in reader:
        row[-1], row[-2] = row[-2], row[-1]
        rows.append(row)

with open(chemin+"affectation_caserne_inverse.csv", mode='w', newline='', encoding='iso-8859-1') as output_file:
    writer = csv.writer(output_file, delimiter=';')
    writer.writerow(header)  
    writer.writerows(rows) 

with open(chemin+"affectation_caserne_inverse.csv", encoding='iso-8859-1') as affectation_file:
    contenu_csv = csv.reader(affectation_file, delimiter=';')
    next(contenu_csv)
    for row in contenu_csv:
        if len(row) >= 3:  
            matricule, codeCaserne, dateAffectation = row 
            requete = "INSERT INTO `estAffecte` (`matriculePers`, `codeCaserne`, `dateAff`) VALUES (%s, %s, %s)"
            valeurs = (matricule, codeCaserne, dateAffectation)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL:", err)


"""
Remplissage de la table appartient
"""
try:
    with open(chemin+"habilitation2.csv", encoding='iso-8859-1') as habilitation_trie_file:
        contenu_csv = csv.reader(habilitation_trie_file, delimiter=';')
        next(contenu_csv) 
        for row in contenu_csv:
            matricule, codeHab, dateHab = row
            
            requete = "INSERT INTO `appartient` (`matriculePers`, `codeHab`, `dateHab`) VALUES (%s, %s, %s)"
            valeurs = (matricule, codeHab, dateHab)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de la table engin
"""

"""
Ce script traite les données de plusieurs fichiers CSV pour produire un nouveau fichier CSV qui contient des informations sur les engins de pompiers, 
leurs casernes et les situations auxquelles ils sont associés.
"""
code_caserne = {}
with open(chemin+"caserne.csv", encoding='iso-8859-1') as caserne_file:
    caserne_csv = csv.reader(caserne_file, delimiter=';')
    next(caserne_csv)
    for row in caserne_csv:
        code_caserne[row[1]] = row[0] 


concatenated_rows = []
with open(chemin+"engin.csv", encoding='iso-8859-1') as engin_file:
    engin_csv = csv.reader(engin_file, delimiter=';')
    header = next(engin_csv) 
    for row in engin_csv:
        lib_caserne = row[2]
        code = code_caserne.get(lib_caserne)
        if code:
            row.append(code)
            del row[2]
            num_ordre = row[1]
            if len(num_ordre) == 1:
                num_ordre = '0' + num_ordre
            id_vehicle = row[0] + num_ordre
            row.append(id_vehicle)
            concatenated_rows.append(row)

with open(chemin+"engin_caserne.csv", mode='w', newline='', encoding='iso-8859-1') as concat_file:
    writer = csv.writer(concat_file, delimiter=';')
    del header[2]
    writer.writerow(header + ["codeCaserne", "idVehicle"])
    writer.writerows(concatenated_rows)

situation_map = {}
with open(chemin+'situation.csv', newline='', encoding='iso-8859-1') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        engins = [engin.strip() for engin in row['engin1'].split(';')] + [engin.strip() for engin in row['engin2'].split(';')] + [engin.strip() for engin in row['engin3'].split(';')]
        for engin in engins:
            if engin:
                code_type_engin = engin.split(';')[0]
                if code_type_engin not in situation_map:
                    situation_map[code_type_engin] = []
                situation_map[code_type_engin].append(row['refSituation'])

with open(chemin+'engin_caserne.csv', newline='', encoding='iso-8859-1') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    header = reader.fieldnames + ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'S10', 'S11', 'S12', 'S13']
    with open(chemin+'engin_caserne_situation.csv', 'w', newline='', encoding='iso-8859-1') as output_file:
        writer = csv.DictWriter(output_file, delimiter=';', fieldnames=header)
        writer.writeheader()
        for row in reader:
            code_type_engin = row['codeTypeEngin']
            situations = [situation for situation in situation_map.get(code_type_engin, [None])][:13]
            for i in range(1, 14):
                row[f'S{i}'] = situations[i-1] if i <= len(situations) else ''
            writer.writerow(row)

try:
    with open(chemin+"engin_caserne_situation.csv", encoding='iso-8859-1') as csvfile:
        contenu_csv = csv.reader(csvfile, delimiter=';')

        next(contenu_csv)

        for row in contenu_csv:
            if len(row) >= 17: 
                code_type_engin, num_ordre, code_caserne, id_vehicle, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13 = row

                S1 = None if S1 == '' else S1
                S2 = None if S2 == '' else S2
                S3 = None if S3 == '' else S3
                S4 = None if S4 == '' else S4
                S5 = None if S5 == '' else S5
                S6 = None if S6 == '' else S6
                S7 = None if S7 == '' else S7
                S8 = None if S8 == '' else S8
                S9 = None if S9 == '' else S9
                S10 = None if S10 == '' else S10
                S11 = None if S11 == '' else S11
                S12 = None if S12 == '' else S12
                S13 = None if S13 == '' else S13

                requete = "INSERT INTO `engin`(`codeTypeEngin`, `numOrdre`, `codeCaserne`, `idVehicule`, `S1`, `S2`, `S3`, `S4`, `S5`, `S6`, `S7`, `S8`, `S9`, `S10`, `S11`, `S12`, `S13`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (code_type_engin, num_ordre, code_caserne, id_vehicle, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13)

                try:
                    with mysql.connector.connect(**params) as db:
                        with db.cursor() as c:
                            c.execute(requete, values)
                            db.commit()
                except mysql.connector.Error as err:
                    print("Erreur MySQL:", err)
            else:
                print("La ligne n'a pas le bon format:", row)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)

"""
Remplissage de moyenHumain 
"""

"""
Ce code lit un fichier CSV appelé "mobiliser_moyens_humains.csv" et stocke son contenu dans une liste appelée "mobiliser_moyens_humains_data". 
Chaque élément de la liste est une ligne du fichier CSV, représentée sous forme de liste de chaînes de caractères.
Ensuite, le code parcourt chaque ligne de la liste et échange les valeurs des deux premières colonnes (c'est-à-dire les éléments 0 et 1 de chaque sous-liste).
Enfin, le code écrit le contenu modifié de la liste dans un nouveau fichier CSV appelé "mobiliser_moyens_humains_modifie.csv", en utilisant le caractère point-virgule comme délimiteur.
"""
mobiliser_moyens_humains_data = []
with open(chemin+"mobiliser_moyens_humains.csv", encoding='iso-8859-1') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    for row in csv_reader:
        mobiliser_moyens_humains_data.append(row)

for row in mobiliser_moyens_humains_data:
    row[0], row[1] = row[1], row[0]

with open(chemin+"mobiliser_moyens_humains_modifie.csv", mode='w', newline='', encoding='iso-8859-1') as modified_file:
    writer = csv.writer(modified_file, delimiter=';')
    writer.writerows(mobiliser_moyens_humains_data)

try:
    with open(chemin+"mobiliser_moyens_humains_modifie.csv", encoding='iso-8859-1') as mobiliser_moyens_humains_file:
        contenu_csv = csv.reader(mobiliser_moyens_humains_file, delimiter=';')
        next(contenu_csv)

        for row in contenu_csv:
            codeFonctionHabilitation, typeEngin, nbPompiers = row

            requete = "INSERT INTO `moyenHumain` (`codeTypeEngin`, `codeHab`, `nbPompier`) VALUES (%s, %s, %s)"
            valeurs = (codeFonctionHabilitation, typeEngin, nbPompiers)

            try:
                with mysql.connector.connect(**params) as db:
                    with db.cursor() as c:
                        c.execute(requete, valeurs)
                        db.commit()
            except mysql.connector.Error as err:
                print("Erreur MySQL:", err)
except FileNotFoundError:
    print("Le fichier CSV n'existe pas.")
except Exception as e:
    print("Une erreur s'est produite:", e)