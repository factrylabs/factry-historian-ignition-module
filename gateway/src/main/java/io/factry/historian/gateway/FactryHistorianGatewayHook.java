package io.factry.historian.gateway;

import com.inductiveautomation.ignition.common.BundleUtil;
import com.inductiveautomation.ignition.common.licensing.LicenseState;
import com.inductiveautomation.ignition.gateway.config.ExtensionPoint;
import com.inductiveautomation.ignition.gateway.model.AbstractGatewayModuleHook;
import com.inductiveautomation.ignition.gateway.model.GatewayContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Gateway hook for the Factry Historian module.
 *
 * This is instantiated by Ignition when the module is loaded in the gateway scope.
 * It registers the Factry Historian extension point so users can create
 * historian profiles through the Gateway UI (Config > Tags > History > Historians).
 */
public class FactryHistorianGatewayHook extends AbstractGatewayModuleHook {
    private static final Logger logger = LoggerFactory.getLogger(FactryHistorianGatewayHook.class);

    private GatewayContext gatewayContext;
    private final FactryHistorianExtensionPoint extensionPoint = new FactryHistorianExtensionPoint();

    @Override
    public void setup(GatewayContext context) {
        logger.info("Factry Historian Module - Setup");
        logger.info("MODULE VERSION: {}", FactryHistorianModule.MODULE_VERSION);

        this.gatewayContext = context;
        extensionPoint.setGatewayContext(context);

        BundleUtil.get().addBundle(FactryHistorianExtensionPoint.class);
    }

    @Override
    public void startup(LicenseState activationState) {
        logger.info("Factry Historian Module - Startup");
        logger.info("MODULE VERSION: {}", FactryHistorianModule.MODULE_VERSION);
        // Historian instances are created by the extension point when users
        // configure them in the Gateway UI. No hardcoded instance needed.
    }

    @Override
    public void shutdown() {
        logger.info("Factry Historian Module - Shutdown");

        BundleUtil.get().removeBundle(FactryHistorianExtensionPoint.class);
    }

    @Override
    public boolean isFreeModule() {
        return true;
    }

    @Override
    public List<? extends ExtensionPoint<?>> getExtensionPoints() {
        return Collections.singletonList(extensionPoint);
    }
}
