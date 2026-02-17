# Sempy Chat Skills

This file defines intent-override rules for the Sempy Chat copilot.
Each rule maps specific phrases in a user's prompt to a fixed function,
bypassing the normal scoring algorithm.

## Rules

To add a new rule, copy an existing block and update the patterns and function name.
Each rule needs a `Patterns` line (comma-separated trigger phrases) and a `Function` line (exact function name).

---

## Semantic Modeling

### Run Model BPA
- **Patterns**: model bpa, best practice analyzer, run bpa
- **Function**: run_model_bpa

### Vertipaq Analyzer
- **Patterns**: vertipaq, verti paq, verti-paq
- **Function**: vertipaq_analyzer

### VPAX
- **Patterns**: vpax, .vpax, create vpax
- **Function**: create_vpax

### Format DAX
- **Patterns**: format dax, dax format, dax formatter
- **Function**: tom.format_dax
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.format_dax()
```

### Translate Semantic Model
- **Patterns**: translate semantic model, translate model, translate dataset, translation
- **Function**: translate_semantic_model
- **Code**:
```python
import sempy_labs as labs
labs.translate_semantic_model(dataset={dataset}, languages={languages}, workspace={workspace})
```

### Refresh Semantic Model
- **Patterns**: refresh semantic model, refresh model, refresh dataset
- **Function**: refresh_semantic_model

### Backup Semantic Model
- **Patterns**: backup semantic model, backup model, backup dataset
- **Function**: backup_semantic_model

### Restore Semantic Model
- **Patterns**: restore semantic model, restore model, restore dataset
- **Function**: restore_semantic_model

### Deploy Semantic Model
- **Patterns**: deploy semantic model, deploy model, deploy dataset
- **Function**: deploy_semantic_model

### Semantic Model Size
- **Patterns**: semantic model size, model size, dataset size, size of model
- **Function**: get_semantic_model_size

### Auto-generate Measure Descriptions
- **Patterns**: generate measure description, generate description, auto generate description, measure description
- **Function**: tom.generate_measure_descriptions
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.generate_measure_descriptions()
```
