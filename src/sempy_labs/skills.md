# Sempy Chat Skills

This file defines intent-override rules for the Sempy Chat copilot.
Each rule maps specific phrases in a user's prompt to a fixed function,
bypassing the normal scoring algorithm.

## Rules

To add a new rule, copy an existing block and update the patterns and function name.
Each rule needs a `Patterns` line (comma-separated trigger phrases) and a `Function` line (exact function name).

---

### Run Model BPA
- **Patterns**: model bpa, best practice analyzer, run bpa
- **Function**: sempy_labs.run_model_bpa

### Vertipaq Analyzer
- **Patterns**: vertipaq, verti paq, verti-paq
- **Function**: sempy_labs.vertipaq_analyzer

### VPAX
- **Patterns**: vpax, .vpax
- **Function**: sempy_labs.create_vpax

### Format DAX
- **Patterns**: format dax, dax format, dax formatter
- **Function**: tom.format_dax
- **Code**:
```python
from sempy_labs.tom import connect_semantic_model
with connect_semantic_model(dataset={dataset}, readonly=False, workspace={workspace}) as tom:
    tom.format_dax()
```

