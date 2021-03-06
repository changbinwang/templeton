#   Licensed to the Apache Software Foundation (ASF) under one or more
#   contributor license agreements.  See the NOTICE file distributed with
#   this work for additional information regarding copyright ownership.
#   The ASF licenses this file to You under the Apache License, Version 2.0
#   (the "License"); you may not use this file except in compliance with
#   the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#
# RPM Spec file for Templeton version @version@
#

%define name         templeton
%define version      @version@
%define release      @package.release@

# Installation Locations
%define _prefix      @package.prefix@
%define _bin_dir     %{_prefix}/bin
%define _conf_dir    @package.conf.dir@
%define _include_dir %{_prefix}/include
%define _lib_dir     %{_prefix}/lib
%define _lib64_dir   %{_prefix}/lib64
%define _libexec_dir %{_prefix}/libexec
%define _log_dir     @package.log.dir@
%define _man_dir     %{_prefix}/man
%define _pid_dir     @package.pid.dir@
%define _sbin_dir    %{_prefix}/sbin
%define _share_dir   %{_prefix}/share/templeton
%define _src_dir     %{_prefix}/src
%define _var_dir     %{_prefix}/var/lib

# Build time settings
%define _build_dir  @package.build.dir@
%define _final_name @final.name@
%define debug_package %{nil}

# Disable brp-java-repack-jars
%if 0%{?suse_version}
%define __os_install_post \
    /usr/lib/rpm/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/brp-strip %{__strip}} \
    /usr/lib/rpm/brp-python-bytecompile %{nil}    
%else
%define __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:/usr/lib/rpm/redhat/brp-strip %{__strip}} \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
    /usr/lib/rpm/brp-python-bytecompile %{nil}
%endif

Summary: Templeton provides a REST-like web API for HCatalog and related Hadoop components.
License: Apache License, Version 2.0
URL: https://issues.apache.org/jira/browse/HCATALOG-182
#Vendor:  TODO
Group: Development/Libraries
Name: %{name}
Version: %{version}
Release: %{release} 
Source0: %{_final_name}.tar.gz
Prefix: %{_prefix}
Prefix: %{_conf_dir}
Prefix: %{_log_dir}
Buildroot: %{_build_dir}
Buildarch: noarch
Requires: hadoop, sh-utils, textutils
AutoReqProv: no
Provides: templeton

%description
Templeton provides a REST-like web API for HCatalog and related Hadoop components. As shown in the figure below, developers make HTTP requests to access Hadoop MapReduce, Pig, Hive, and HCatalog DDL from within applications. Data and code used by Templeton is maintained in HDFS. HCatalog DDL commands are executed directly when requested. MapReduce, Pig, and Hive jobs are placed in queue by Templeton and can be monitored for progress or stopped as required. Developers specify a location in HDFS into which Templeton should place Pig, Hive, and MapReduce results.

#%package conf-pseudo
#Summary: Default Hadoop configuration templates
#Group: System/Daemons
#Requires: %{name} == %{version}
#
#%description conf-pseudo
#Installation of this RPM will setup your machine to run in pseudo-distributed mode
#where each Hadoop daemon runs in a separate Java process.

%prep
%setup -n %{_final_name}

#%build

if [ -d ${RPM_BUILD_DIR}%{_prefix} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_prefix}
fi

if [ -d ${RPM_BUILD_DIR}%{_log_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_log_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_conf_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_conf_dir}
fi

if [ -d ${RPM_BUILD_DIR}%{_pid_dir} ]; then
  rm -rf ${RPM_BUILD_DIR}%{_pid_dir}
fi

mkdir -p ${RPM_BUILD_DIR}%{_prefix}
mkdir -p ${RPM_BUILD_DIR}%{_log_dir}
mkdir -p ${RPM_BUILD_DIR}%{_conf_dir}


#########################
#### INSTALL SECTION ####
#########################
%install
mv ${RPM_BUILD_DIR}/%{_final_name}/etc/templeton/* ${RPM_BUILD_DIR}%{_conf_dir}
rm -rf ${RPM_BUILD_DIR}/%{_final_name}/etc/templeton
mv ${RPM_BUILD_DIR}/%{_final_name}/* ${RPM_BUILD_DIR}%{_prefix}
#cp -f ${RPM_BUILD_DIR}/../SOURCES/%{_final_name}.jar ${RPM_BUILD_DIR}%{_share_dir}/%{_final_name}.jar

%pre

# %post
# bash ${RPM_INSTALL_PREFIX0}/sbin/update-templeton-env.sh \
#        --prefix=${RPM_INSTALL_PREFIX0} \
#        --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
#        --conf-dir=${RPM_INSTALL_PREFIX1} \
#        --log-dir=${RPM_INSTALL_PREFIX2}

# %preun
# bash ${RPM_INSTALL_PREFIX0}/sbin/update-templeton-env.sh \
#        --prefix=${RPM_INSTALL_PREFIX0} \
#        --bin-dir=${RPM_INSTALL_PREFIX0}/bin \
#        --conf-dir=${RPM_INSTALL_PREFIX1} \
#        --log-dir=${RPM_INSTALL_PREFIX2} \
#        --uninstall

%files 
%defattr(-,root,root)
%attr(0755,root,hadoop) %{_log_dir}
%config %{_conf_dir}
%{_prefix}

