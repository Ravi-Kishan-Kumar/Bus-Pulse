# AI models

This folder contains the trained model artifacts used by the stream processor.

- `occupancy_lgbm.txt` is the LightGBM occupancy model.
- `occupancy_lgbm.meta.json` records the model's training report and metadata.

The `ml-train` Compose service writes updated artifacts here. The folder is bind-mounted into the processor container at `/app/models`.
