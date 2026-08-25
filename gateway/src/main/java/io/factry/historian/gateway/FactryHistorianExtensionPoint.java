package io.factry.historian.gateway;

import com.inductiveautomation.historian.gateway.api.Historian;
import com.inductiveautomation.historian.gateway.api.HistorianExtensionPoint;
import com.inductiveautomation.historian.gateway.api.HistorianProvider;
import com.inductiveautomation.historian.gateway.api.config.HistorianSettings;
import com.inductiveautomation.ignition.gateway.config.DecodedResource;
import com.inductiveautomation.ignition.gateway.config.ExtensionPointConfig;
import com.inductiveautomation.ignition.gateway.dataroutes.openapi.SchemaUtil;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import com.inductiveautomation.ignition.gateway.web.nav.ExtensionPointResourceForm;
import com.inductiveautomation.ignition.gateway.web.nav.WebUiComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Extension point for the Factry Historian.
 *
 * This class registers the Factry Historian as a historian type that can be
 * created and managed through the Ignition Gateway UI at:
 * Config → Services → Historians → Create New Historian Profile
 */
public class FactryHistorianExtensionPoint extends HistorianExtensionPoint<FactryHistorianSettings> {
    private static final Logger logger = LoggerFactory.getLogger(FactryHistorianExtensionPoint.class);

    /**
     * Unique type identifier for this historian.
     * This ID is used internally by Ignition to identify the historian type.
     */
    public static final String TYPE_ID = "factry-historian";

    /**
     * Display name for the historian type.
     * This appears in the Gateway UI when selecting a historian type.
     */
    public static final String DISPLAY_NAME = "FactryHistorianExtensionPoint.HistorianType.Name";

    /**
     * Description key for the historian type (resolved via BundleUtil).
     */
    public static final String DESCRIPTION = "FactryHistorianExtensionPoint.HistorianType.Description";

    /**
     * Creates a new Factry Historian extension point.
     * Passes literal display strings to the parent constructor.
     */
    public FactryHistorianExtensionPoint() {
        super(TYPE_ID, DISPLAY_NAME, DESCRIPTION);
        logger.debug("Factry Historian Extension Point created");
        logger.debug("MODULE VERSION: {}", FactryHistorianModule.MODULE_VERSION);
        logger.debug("Type: {}, Name: {}", TYPE_ID, DISPLAY_NAME);
    }


    /**
     * Provide the settings type explicitly so the framework can decode config JSON.
     * This is separate from defaultSettings() so we can return empty defaults
     * without losing the type information.
     */
    @Override
    public Optional<Class<FactryHistorianSettings>> settingsType() {
        return Optional.of(FactryHistorianSettings.class);
    }

    /**
     * Provide populated defaults for the create form.
     * <p>
     * NOTE: In Ignition 8.3.3 returning non-empty defaults here triggered
     * form.reset(defaultSettings) on mount, which wiped config.profile.type from
     * the form state and broke the edit sidebar with "Extension Point Form Not
     * Found". We are re-testing this on 8.3.6 to see whether that bug is fixed.
     */
    @Override
    public Optional<FactryHistorianSettings> defaultSettings() {
        // The commented out code should work, but there is an ignition bug 
        // if we use the code in the comment, the editing existing historian 
        // form will be broken with message: Extension Point Form Not Found
        // FactryHistorianSettings defaults = new FactryHistorianSettings();
        // defaults.setToken("");
        // defaults.setUseTls(true);
        // defaults.setSkipTlsVerification(false);
        // defaults.setDebugLogging(false);
        // defaults.setDelimiter("/");
        // return Optional.of(defaults);
        return Optional.empty();
    }

    /**
     * Provides the web UI component for configuring the historian.
     *
     * This method returns the form definition that Ignition uses to render
     * the configuration page in the Gateway UI.
     *
     * @param type The type of UI component requested
     * @return Optional containing the web UI component configuration
     */
    @Override
    public Optional<WebUiComponent> getWebUiComponent(ComponentType type) {
        logger.debug("getWebUiComponent called: type={}, resourceType={}", type, resourceType());

        try {
            var schema = SchemaUtil.fromType(FactryHistorianConfig.class);
            logger.debug("Schema created successfully: {}", schema);

            var component = new ExtensionPointResourceForm(
                    resourceType(),  // Use the historian resource type from parent class
                    "Factry Historian Configuration",  // Form title
                    TYPE_ID,  // Extension point type ID
                    null,    // no profile-level parameters
                    schema,  // Our config schema
                    Set.of()  // No additional capabilities
            );

            logger.debug("ExtensionPointResourceForm created successfully");
            return Optional.of(component);
        } catch (Exception e) {
            logger.error("Error creating web UI component", e);
            return Optional.empty();
        }
    }

    /**
     * Factory method called by Ignition to create a new historian instance.
     *
     * This is called when:
     * - A user creates a new Factry Historian profile in the Gateway UI
     * - The Gateway loads existing Factry Historian profiles on startup
     *
     * @param context The gateway context
     * @param resource The decoded resource containing the historian configuration
     * @return A new Historian instance
     * @throws Exception if the historian cannot be created
     */
    @Override
    public Historian<FactryHistorianSettings> createHistorianProvider(
            GatewayContext context,
            DecodedResource<ExtensionPointConfig<HistorianProvider, HistorianSettings>> resource
    ) throws Exception {
        logger.info("Creating Factry Historian provider from extension point");

        String historianName = resource.name();

        // The settings are decoded as our concrete FactryHistorianSettings type
        // thanks to defaultSettings() providing the type information
        FactryHistorianSettings settings = resource.config().settings()
                .filter(s -> s instanceof FactryHistorianSettings)
                .map(s -> (FactryHistorianSettings) s)
                .orElseGet(() -> {
                    logger.warn("Settings not of expected type, using defaults");
                    return new FactryHistorianSettings();
                });

        logger.info("Creating Factry Historian: name={}, settings={}", historianName, settings);

        FactryHistoryProvider provider = new FactryHistoryProvider(context, historianName, settings);

        logger.info("Factry Historian provider created successfully");
        return provider;
    }
}
