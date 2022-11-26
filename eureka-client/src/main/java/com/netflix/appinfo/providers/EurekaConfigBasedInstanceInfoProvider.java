package com.netflix.appinfo.providers;

import javax.inject.Singleton;
import javax.inject.Provider;
import java.util.Map;

import com.google.inject.Inject;
import com.netflix.appinfo.DataCenterInfo;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.appinfo.InstanceInfo.PortType;
import com.netflix.appinfo.LeaseInfo;
import com.netflix.appinfo.RefreshableInstanceConfig;
import com.netflix.appinfo.UniqueIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 * InstanceInfo provider that constructs the InstanceInfo this this instance using
 * EurekaInstanceConfig.
 *
 * This provider is @Singleton scope as it provides the InstanceInfo for both DiscoveryClient
 * and ApplicationInfoManager, and need to provide the same InstanceInfo to both.
 *
 * <p>
 *      * 基于建造者模式构造服务实例
 *  * 看 new EurekaConfigBasedInstanceInfoProvider(instanceConfig).get() 这段代码，在 get 方法中完成了服务实例信息的构造。
 *  * 它这里主要用到了建造者设计模式来构建 LeaseInfo 和 InstanceInfo，以 InstanceInfo 为例，它的内部有一个静态的 Builder 类，通过 newBuilder() 方法创建了 InstanceInfo 对象，
 *  * 然后可以调用 Builder 的属性设置方法来设置属性，在设置这些属性的时候，会做一些关联性的校验，在设置完成后，就调用 build() 方法返回对象，
 *  * 也可以在 build 方法中再做一些最终的校验。建造者模式就很适合用于构建这种复杂的对象。
 * </p>


 * @author elandau
 *
 */
@Singleton
public class EurekaConfigBasedInstanceInfoProvider implements Provider<InstanceInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(EurekaConfigBasedInstanceInfoProvider.class);

    private final EurekaInstanceConfig config;

    private InstanceInfo instanceInfo;

    @Inject(optional = true)
    private VipAddressResolver vipAddressResolver = null;


    @Inject
    public EurekaConfigBasedInstanceInfoProvider(EurekaInstanceConfig config) {
        this.config = config;
    }

    @Override
    public synchronized InstanceInfo get() {
        if (instanceInfo == null) {
            // Build the lease information to be passed to the server based on config
            // 续约信息：主要有续约间隔时间（默认30秒）和续约过期时间（默认90秒）
            LeaseInfo.Builder leaseInfoBuilder = LeaseInfo.Builder.newBuilder()
                    .setRenewalIntervalInSecs(config.getLeaseRenewalIntervalInSeconds())
                    .setDurationInSecs(config.getLeaseExpirationDurationInSeconds());

            if (vipAddressResolver == null) {
                vipAddressResolver = new Archaius1VipAddressResolver();
            }

            // Builder the instance information to be registered with eureka server
            // 基于建造者模式来创建 InstanceInfo
            InstanceInfo.Builder builder = InstanceInfo.Builder.newBuilder(vipAddressResolver);

            // set the appropriate id for the InstanceInfo, falling back to datacenter Id if applicable, else hostname
            String instanceId = config.getInstanceId();
            if (instanceId == null || instanceId.isEmpty()) {
                DataCenterInfo dataCenterInfo = config.getDataCenterInfo();
                if (dataCenterInfo instanceof UniqueIdentifier) {
                    instanceId = ((UniqueIdentifier) dataCenterInfo).getId();
                } else {
                    instanceId = config.getHostName(false);
                }
            }

            String defaultAddress;
            if (config instanceof RefreshableInstanceConfig) {
                // Refresh AWS data center info, and return up to date address
                defaultAddress = ((RefreshableInstanceConfig) config).resolveDefaultAddress(false);
            } else {
                defaultAddress = config.getHostName(false);
            }

            // fail safe
            if (defaultAddress == null || defaultAddress.isEmpty()) {
                defaultAddress = config.getIpAddress();
            }

            // 设置属性
            builder.setNamespace(config.getNamespace())
                    .setInstanceId(instanceId)
                    .setAppName(config.getAppname())
                    .setAppGroupName(config.getAppGroupName())
                    .setDataCenterInfo(config.getDataCenterInfo())
                    .setIPAddr(config.getIpAddress())
                    .setHostName(defaultAddress)
                    .setPort(config.getNonSecurePort())
                    .enablePort(PortType.UNSECURE, config.isNonSecurePortEnabled())
                    .setSecurePort(config.getSecurePort())
                    .enablePort(PortType.SECURE, config.getSecurePortEnabled())
                    .setVIPAddress(config.getVirtualHostName())
                    .setSecureVIPAddress(config.getSecureVirtualHostName())
                    .setHomePageUrl(config.getHomePageUrlPath(), config.getHomePageUrl())
                    .setStatusPageUrl(config.getStatusPageUrlPath(), config.getStatusPageUrl())
                    .setASGName(config.getASGName())
                    .setHealthCheckUrls(config.getHealthCheckUrlPath(),
                            config.getHealthCheckUrl(), config.getSecureHealthCheckUrl());


            // Start off with the STARTING state to avoid traffic
            if (!config.isInstanceEnabledOnit()) {
                InstanceStatus initialStatus = InstanceStatus.STARTING;
                LOG.info("Setting initial instance status as: {}", initialStatus);
                builder.setStatus(initialStatus);
            } else {
                LOG.info("Setting initial instance status as: {}. This may be too early for the instance to advertise "
                         + "itself as available. You would instead want to control this via a healthcheck handler.",
                         InstanceStatus.UP);
            }

            // Add any user-specific metadata information
            for (Map.Entry<String, String> mapEntry : config.getMetadataMap().entrySet()) {
                String key = mapEntry.getKey();
                String value = mapEntry.getValue();
                // only add the metadata if the value is present
                if (value != null && !value.isEmpty()) {
                    builder.add(key, value);
                }
            }
            // 调用 build 方法做属性校验并创建 InstanceInfo 实例
            instanceInfo = builder.build();
            instanceInfo.setLeaseInfo(leaseInfoBuilder.build());
        }
        return instanceInfo;
    }

}
