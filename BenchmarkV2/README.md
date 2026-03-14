# Benchmark ETL vs Architecture Fédérée
## Basé sur les requêtes de production réelles – SQL Server / C# / .NET 9

---

## Structure du projet

```
BenchmarkV2/
├── BenchmarkCore/          # Modèles, métriques, stats, export
├── ETLPipeline/            # Requêtes T1–T4 sur base centralisée + BulkLoader T6
├── FederatedQuery/
│   ├── LinkedServer/       # Option A – jointures via Linked Server SQL Server
│   └── InMemory/           # Option B – fetch parallèle + jointures LINQ
├── BenchmarkRunner/        # Orchestrateur principal
├── DatabaseSetup/          # Création des 3 bases et schémas
└── DataGenerator/          # Générateur de données custom (remplace dbgen)
```

---

## Distribution des bases

| Base | Nom SQL | Tables |
|------|---------|--------|
| **Base A – Référentiel** | `Bench_Referentiel` | Type_pièce, Type_mesure, lien_type_mesure_type_pièce, Postes, Personnes |
| **Base B – Production**  | `Bench_Production`  | SN, Infos, Mesures, Historique, Dérogations, Etiquettes |
| **Centralisée – ETL**    | `Bench_Centralized` | Toutes les tables réunies |

---

## Scénarios de test

| Test | Type | Requête réelle |
|------|------|----------------|
| **T1** | SELECT simple | `SELECT SN FROM SN WHERE Is_Active=1 ORDER BY ID_SN DESC` |
| **T2** | SELECT 4 jointures | Mesures par pièce (SN → lien → Type_mesure → Dérogations) |
| **T3** | SELECT CTE + fenêtrage | Historique complet avec ROW_NUMBER, LAG, UNION ALL |
| **T4** | INSERT + OUTPUT | `INSERT INTO Mesures ... OUTPUT INSERTED.ID_Mesure` |
| **T5** | Charge en rafale | T2 × {10, 25, 50} exécutions parallèles via Task.WhenAll |
| **T6** | Ingestion batch ETL | SqlBulkCopy Mesures Production → Centralisée |

---

## Installation

### 1. Compiler
```powershell
cd BenchmarkV2
dotnet build -c Release
```

### 2. Créer les bases
```powershell
cd DatabaseSetup\bin\Release\net9.0
.\DatabaseSetup.exe localhost

# Avec Linked Server si 2 instances distinctes :
.\DatabaseSetup.exe localhost --linked-server SERVEUR_SECONDAIRE
```

### 3. Générer les données
```powershell
cd DataGenerator\bin\Release\net9.0

# Choisir le volume :
.\DataGenerator.exe "Server=localhost;Database=Bench_Referentiel;..." ^
                    "Server=localhost;Database=Bench_Production;..." ^
                    "Server=localhost;Database=Bench_Centralized;..." ^
                    small

# small  → ~1 000 SN, ~20 000 mesures   (~quelques secondes)
# medium → ~10 000 SN, ~250 000 mesures (~quelques minutes)
# large  → ~100 000 SN, ~3M mesures     (~15-30 minutes)
```

### 4. Configurer appsettings.json
```json
{
  "Benchmark": {
    "ReferentielConnectionString": "Server=localhost;Database=Bench_Referentiel;...",
    "ProductionConnectionString":  "Server=localhost;Database=Bench_Production;...",
    "CentralizedConnectionString": "Server=localhost;Database=Bench_Centralized;..."
  },
  "TestSelection": {
    "Scale": "small"
  }
}
```

### 5. Lancer le benchmark
```powershell
cd BenchmarkRunner\bin\Release\net9.0
.\BenchmarkRunner.exe
```

---

## Résultats

Le dossier `Results\` contient après exécution :
```
Results\
├── raw_YYYYMMDD_HHMMSS.csv      # Toutes les mesures brutes (1 ligne par run)
├── summary_YYYYMMDD_HHMMSS.csv  # Médiane, P25, P75, IQR par scénario
└── report_YYYYMMDD_HHMMSS.md    # Rapport Markdown formaté pour le mémoire
```

---

## Protocole de mesure

1. **Warmup** — 1 exécution ignorée (chauffe les caches et plans d'exécution)
2. **Flush cache** — `DBCC DROPCLEANBUFFERS` + `DBCC FREEPROCCACHE` (si droits sysadmin)
3. **10 mesures** consécutives avec sampling CPU toutes les 200ms
4. **Statistiques** — Médiane (P50) comme valeur de référence, IQR (P75–P25) pour la stabilité

---

## Note sur le Linked Server (instance unique)

Sur une machine de développement avec une seule instance SQL Server,
le Linked Server peut pointer sur la même instance :

```sql
EXEC sp_addlinkedserver
    @server  = N'BaseReferentiel',
    @datasrc = N'localhost';  -- ou le nom de ton instance
```

Les requêtes fédérées fonctionneront mais la latence réseau sera nulle —
ce cas est documenté comme limite méthodologique dans le mémoire.
