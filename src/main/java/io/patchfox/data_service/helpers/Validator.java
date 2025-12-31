package io.patchfox.data_service.helpers;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Validator {

    // only leters, numbers, and underscore 
    public static final String DATASET_NAME_REGEX = "^[a-zA-Z0-9_]+$";

    public static final Integer MAX_DATASET_NAME_LENGTH = 255;

    public static boolean validateDatasetNameArg(String datasetName) {
        if (datasetName.length() > MAX_DATASET_NAME_LENGTH) {
            log.warn(
                "datasetName arg of length {} exceeds max length allowed: {}", 
                datasetName.length(), 
                MAX_DATASET_NAME_LENGTH
            );

            return false;
        }

        if ( !datasetName.matches(DATASET_NAME_REGEX)) {
            log.warn(
                "datasetName arg {} does not conform to regex pattern: {}", 
                datasetName, 
                DATASET_NAME_REGEX
            );

            return false;            
        }

        return true;
    }

}
