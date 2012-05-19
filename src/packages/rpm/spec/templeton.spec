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
BuildRoot: %{_build_dir}
BuildArch: noarch
Requires: hadoop, sh-utils, textutils
AutoReqProv: no
Provides: templeton

%description
Templeton provides a REST-like web API for HCatalog and related Hadoop components. As shown in the figure below, developers make HTTP requests to access Hadoop MapReduce, Pig, Hive, and HCatalog DDL from within applications. Data and code used by Templeton is maintained in HDFS. HCatalog DDL commands are executed directly when requested. MapReduce, Pig, and Hive jobs are placed in queue by Templeton and can be monitored for progress or stopped as required. Developers specify a location in HDFS into which Templeton should place Pig, Hive, and MapReduce results.

%prep
%setup -n %{_final_name}

%install
install -d ${RPM_BUILD_ROOT}%{_prefix} 
install -d ${RPM_BUILD_ROOT}%{_conf_dir} 
install -d ${RPM_BUILD_ROOT}%{_log_dir} 

mv ${RPM_BUILD_DIR}/%{_final_name}/etc/templeton/* ${RPM_BUILD_ROOT}%{_conf_dir}
rm -rf ${RPM_BUILD_DIR}/%{_final_name}/etc/templeton
mv ${RPM_BUILD_DIR}/%{_final_name}/* ${RPM_BUILD_ROOT}%{_prefix}

%pre

%files 
%defattr(-,root,root)
%attr(0755,root,hadoop) %{_log_dir}
%config %{_conf_dir}
%{_prefix}
