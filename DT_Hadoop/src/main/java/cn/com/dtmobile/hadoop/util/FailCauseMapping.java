package cn.com.dtmobile.hadoop.util;

import java.util.HashSet;
import java.util.Set;

/**
 * S1MME失败原因映射 2017-06-15
 * 
 * @author zhangchao15
 * 
 */
public class FailCauseMapping {
	//NAS
	public final static int attach = 1 ;
	public final static int sevice_request = 2; 
	public final static int extended_serviceRequest = 3; 
	public final static int tau = 5; 
	public final static int detach = 6; 
	public final static int pdn_Connectivity = 7; 
	public final static int pdn_Disconnection = 8; 
	public final static int eps_bearer_resource_allocation = 9; 
	public final static int eps_bearer_resource_modification = 10;
	public final static int eps_bearer_context_deactivation = 11;
	public final static int eps_bearer_context_modification = 12;
	public final static int dedicated_eps_bearer_context_activation = 13;
	public final static int identity_acquisition  = 29;
	public final static int authentication  = 30;
	public final static int security_activation  = 31;
	public final static int sms = 32;
	public final static int cs_service_notification = 33;
	//S1AP
	public final static int  paging   =  4; 
	public final static int  x2_handover  = 14;
	public final static int  s1_andover_in   = 15;
	public final static int  s1_handover_out   = 16;
	public final static int  s1_handover_cancel  = 17;
	public final static int  initial_context_setup  = 18;
	public final static int  ue_context_modification = 19;
	public final static int  ue_context_release  = 20;
	public final static int  e_rab_release   = 21;
	public final static int  reset   = 22;
	public final static int  error_indication  = 23;
	public final static int  s1_setup   = 24;
	public final static int  enb_configuration_update = 25;
	public final static int  mme_configuration_update = 26;
	public final static int  overload_start    = 27;
	public final static int  overload_stop  = 28;
	
	private static int[] nasArray={attach,sevice_request,extended_serviceRequest,tau,detach,pdn_Connectivity,pdn_Disconnection,
			eps_bearer_resource_allocation,eps_bearer_resource_modification,eps_bearer_context_deactivation,
			eps_bearer_context_modification,dedicated_eps_bearer_context_activation,identity_acquisition,authentication,
			security_activation};
	private static int[] s1apArray={paging,x2_handover,s1_andover_in,s1_handover_out,s1_handover_cancel,initial_context_setup,
			ue_context_modification,ue_context_release,ue_context_release,e_rab_release,reset,error_indication,
			s1_setup,enb_configuration_update,mme_configuration_update,overload_start,overload_stop,sms,cs_service_notification};
	 
	private static Set<Integer> nasSet = new HashSet<Integer>();
	private static Set<Integer> s1apSet = new HashSet<Integer>();
	static {
		for (int i = 0; i < nasArray.length; i++) {
			nasSet.add(nasArray[i]);
		}
		for (int i = 0; i < s1apArray.length; i++) {
			s1apSet.add(s1apArray[i]);
		}
	}
	public static int getFailCauseMapping(int causeGroup, int causeSpecific,
			int failType) {
		//全FFFFFFFFF
		int cause = 65535;
		// S1ap
		if (s1apSet.contains(failType)) {
			switch (causeGroup) {
			case 1:
				switch (causeSpecific) {
				case 1:
					cause = 1;
					break;
				case 2:
					cause = 2;
					break;
				case 3:
					cause = 3;
					break;
				case 4:
					cause = 4;
					break;
				case 5:
					cause = 5;
					break;
				case 6:
					cause = 6;
					break;
				case 7:
					cause = 7;
					break;
				case 8:
					cause = 8;
					break;
				case 9:
					cause = 9;
					break;
				case 10:
					cause = 10;
					break;
				case 11:
					cause = 11;
					break;
				case 12:
					cause = 12;
					break;
				case 13:
					cause = 13;
					break;
				case 14:
					cause = 14;
					break;
				case 15:
					cause = 15;
					break;
				case 16:
					cause = 16;
					break;
				case 17:
					cause = 17;
					break;
				case 18:
					cause = 18;
					break;
				case 19:
					cause = 19;
					break;
				case 20:
					cause = 20;
					break;
				case 21:
					cause = 21;
					break;
				case 22:
					cause = 22;
					break;
				case 23:
					cause = 23;
					break;
				case 24:
					cause = 24;
					break;
				case 25:
					cause = 25;
					break;
				case 26:
					cause = 26;
					break;
				case 27:
					cause = 27;
					break;
				case 28:
					cause = 28;
					break;
				case 29:
					cause = 29;
					break;
				case 30:
					cause = 30;
					break;
				case 31:
					cause = 31;
					break;
				case 32:
					cause = 32;
					break;
				case 33:
					cause = 33;
					break;
				case 34:
					cause = 34;
					break;
				case 35:
					cause = 35;
					break;
				case 36:
					cause = 128;
					break;
				case 37:
					cause = 129;
					break;
				case 38:
					cause = 130;
					break;
				case 254:
					cause = 0;
					break;
				}
				break;
			case 2:
				switch (causeSpecific) {
				case 1:
					cause = 256;
					break;
				case 254:
					cause = 257;
					break;
				}
				break;
			case 3:
				switch (causeSpecific) {
				case 1:
					cause = 512;
					break;
				case 2:
					cause = 513;
					break;
				case 3:
					cause = 514;
					break;
				case 4:
					cause = 640;
					break;
				case 254:
					cause = 515;
					break;
				}
				break;
			case 4:
				switch (causeSpecific) {
				case 1:
					cause = 768;
					break;
				case 2:
					cause = 769;
					break;
				case 3:
					cause = 770;
					break;
				case 4:
					cause = 771;
					break;
				case 5:
					cause = 772;
					break;
				case 6:
					cause = 773;
					break;
				case 254:
					cause = 774;
					break;
				}
				break;
			case 5:
				switch (causeSpecific) {
				case 1:
					cause = 1024;
					break;
				case 2:
					cause = 1026;
					break;
				case 3:
					cause = 1027;
					break;
				case 4:
					cause = 1025;
					break;
				case 5:
					cause = 1029;
					break;
				case 254:
					cause = 1028;
					break;
				}
				break;
			}

			// NAS
		} else if (nasSet.contains(failType)) {
			switch (causeGroup) {
			case 1:
				cause = causeSpecific;
				break;
			case 2:
				cause = causeSpecific + 256;
				break;
			}
		}
		return cause;
	}
	public static void main(String[] args) {
		System.out.println(FailCauseMapping.getFailCauseMapping(1, 36, 20));
		System.out.println(FailCauseMapping.getFailCauseMapping(2, 1, 20));
		System.out.println(FailCauseMapping.getFailCauseMapping(3, 254, 18));
		System.out.println(FailCauseMapping.getFailCauseMapping(4, 1, 5));
		System.out.println(FailCauseMapping.getFailCauseMapping(5, 5, 18));
		
	}
}
