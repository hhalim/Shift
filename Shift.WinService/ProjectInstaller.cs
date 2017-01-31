using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Configuration.Install;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace ShiftWinService
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : System.Configuration.Install.Installer
    {
        public ProjectInstaller()
        {
            InitializeComponent();
            this.Installers.Add(GetServiceInstaller());
        }

        private ServiceInstaller GetServiceInstaller()
        {
            var serviceName = GetServiceName();
            var serviceInstaller = new ServiceInstaller();
            serviceInstaller.DisplayName = serviceName;
            serviceInstaller.ServiceName = serviceName;
            return serviceInstaller;
        }

        private string GetServiceName()
        {
            var service = Assembly.GetAssembly(typeof(ProjectInstaller));
            var config = ConfigurationManager.OpenExeConfiguration(service.Location);
            var serviceName = config.AppSettings.Settings["ServiceName"].Value;
            var processID = config.AppSettings.Settings["ShiftPID"].Value;
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new IndexOutOfRangeException("Configuration for ServiceName is missing or invalid.");

            if (string.IsNullOrWhiteSpace(processID))
                throw new IndexOutOfRangeException("Configuration for ShiftPID is missing or invalid.");

            return serviceName;
        }

        private void serviceProcessInstaller_AfterInstall(object sender, InstallEventArgs e)
        {

        }
    }
}
