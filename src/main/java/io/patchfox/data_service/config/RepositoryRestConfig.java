package io.patchfox.data_service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.rest.core.config.RepositoryRestConfiguration;
import org.springframework.data.rest.core.mapping.ExposureConfiguration;
import org.springframework.data.rest.webmvc.config.RepositoryRestConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.http.HttpMethod;

@Configuration
public class RepositoryRestConfig implements RepositoryRestConfigurer {
    
    @Override
    public void configureRepositoryRestConfiguration(RepositoryRestConfiguration config, CorsRegistry cors) {
        ExposureConfiguration exposureConfig = config.getExposureConfiguration();
        
        // Disable all write operations globally
        exposureConfig
            .withItemExposure((metadata, httpMethods) -> 
                httpMethods.disable(HttpMethod.PUT, HttpMethod.PATCH, HttpMethod.POST, HttpMethod.DELETE))
            .withCollectionExposure((metadata, httpMethods) -> 
                httpMethods.disable(HttpMethod.POST, HttpMethod.DELETE));
    }
}

