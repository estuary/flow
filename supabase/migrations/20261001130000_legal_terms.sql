begin;

create type internal.legal_terms_type as enum ('msa', 'privacy_policy');

create table internal.legal_terms (
    id public.flowid primary key not null default internal.id_generator(),
    type internal.legal_terms_type not null,
    text text not null,
    version integer not null check (version > 0),
    created_at timestamptz not null default now(),
    unique (type, version)
);

-- Users accept a specific (type, version) of the terms, so published text must
-- never change. New terms are published as a new version.
create function internal.legal_terms_reject_update() returns trigger
language plpgsql as $$
begin
    raise exception 'legal terms are immutable; insert a new version';
end;
$$;

create trigger legal_terms_reject_update
    before update on internal.legal_terms
    for each row execute function internal.legal_terms_reject_update();

-- Each type has its own version sequence, starting at 1. An insert that doesn't
-- define a version gets the next version number of its type.
create function internal.legal_terms_assign_version() returns trigger
language plpgsql as $$
begin
    if new.version is not null then
        return new;
    end if;

    select coalesce(max(version), 0) + 1 into new.version
    from internal.legal_terms
    where type = new.type;
    return new;
end;
$$;

create trigger legal_terms_assign_version
    before insert on internal.legal_terms
    for each row execute function internal.legal_terms_assign_version();

-- Source: estuary/ui public/terms.html (effective March 12, 2026).
insert into internal.legal_terms (type, version, text) values (
    'msa',
    1,
    $legal_terms$# MASTER SERVICES AGREEMENT

Effective as of March 12, 2026

This Master Services Agreement (“Agreement”) is agreed and entered into by Estuary Technologies, Inc., a Delaware corporation (“Company”, “Estuary”, “we”, “us”, “our”), and the party subscribing to the Services (defined below) (“Customer”, “you”, “your”). This Agreement is effective as of the earlier of the date on which Customer first registers for Services or the date on which the Parties execute the first Order Form (defined below) (the “Effective Date”). This Agreement includes and incorporates the terms and conditions below, any signed Order Forms, and to the extent applicable, the Data Processing Agreement (available at [https://estuary.dev/legal/dpa/](https://estuary.dev/legal/dpa/)) (the “Data Processing Agreement”) and it contains, among other things, warranty disclaimers, liability limitations and use limitations.

If you are entering into this Agreement on behalf of a legal entity, you represent that you have the authority to bind such entity to this Agreement, in which case the terms “you” or “your” refer to such entity. Customer and Estuary are each referred to herein as a “Party,” and together are referred to herein as the “Parties.”

BY ACCEPTING THIS AGREEMENT OR ACCESSING SERVICES, YOU ACKNOWLEDGE THAT YOU HAVE REVIEWED AND AGREE TO BE LEGALLY BOUND BY THE TERMS AND CONDITIONS OF THIS AGREEMENT. IF YOU DO NOT ACCEPT THE TERMS OF THIS AGREEMENT, OR DO NOT HAVE THE AUTHORITY TO BIND THE ENTITY TO THIS AGREEMENT, YOU AND YOUR AUTHORIZED USERS MAY NOT ACCESS OR USE THE SERVICES.

## 1. Services

**1.1.** Services. Estuary will use commercially reasonable efforts to provide the Services to Customer (as defined below), and Customer may use and access such Services, in each case subject to the terms of this Agreement. The term “Services” includes Estuary’s user interface, APIs, data-capture, data-transformation, and data-materialization services, implementation and professional services, maintenance and support, the Software, and such other services provided by Estuary or its Affiliates, in each case to the extent laid out in the applicable Order Form or as provided as part of a Trial Subscription.

**1.2.** Trial Subscription. Estuary may provide a trial, free or introductory version of the Services, free of charge (a “Trial Subscription”). The scope of Services for a Trial Subscription will be determined by the Company from time to time, in its sole discretion. By accessing the Services under a Trial Subscription (or by authorizing its Authorized Users to so access the Services under a Trial Subscription), Customer agrees that the terms of this Agreement will apply with respect to its usage of such Trial Subscription; provided, however, that Estuary will not provide indemnification under Section 8 hereof with respect to such usage.

**1.3.** Restrictions on Use. Customer will not (and will not authorize or permit any third party to) make any use or disclosure of the Services, Software or Documentation that is not expressly permitted under this Agreement or any applicable Order Form. Without limiting the foregoing, Customer will not (and will not authorize or permit any third party to): (i) reverse engineer, decompile, disassemble, or otherwise attempt to discern the source code of the Software, (ii) modify, adapt, or translate the Software, (iii) resell, distribute, or license the Software, make the Software available on a “service bureau” basis, or otherwise allow any third party to use or access the Software (other than Authorized Users to the extent permitted hereunder), (iv) remove or modify any proprietary marking or restrictive legends placed on the Software or the Documentation, or (v) use the Software in violation of any applicable law or regulation or for any purpose not contemplated by the applicable Documentation, including, without limitation, uploading or submitting any personal health information as defined by the Health Insurance Portability and Accountability Act of 1996 (“HIPAA”) unless Customer and Estuary have entered a Business Associate Agreement, which would be incorporated by reference into, and made subject to, this Agreement.

**1.4.** Customer Data. Customer will retain ownership of all rights to any electronic data submitted by Customer in connection with the Services and the results of processing such data (collectively, the “Customer Data”). Notwithstanding the foregoing, Estuary shall have the right to collect and analyze data and other information relating to the provision, use and performance of various aspects of the Services and (including, without limitation, information concerning Customer Data and data derived therefrom), and Estuary shall have the right (during and after the term hereof) to use such information and data to improve and enhance the Software and for other development, diagnostic and corrective purposes in connection with the Software and other Company offerings, and (ii) disclose such data solely in aggregate, or other fully de-identified form in connection with its business. Notwithstanding the foregoing, Estuary’s collection and use of Customer Data for the provision of Services under this Agreement is governed by the terms of the Data Processing Agreement.

**1.5.** Title. As between Estuary and Customer, Estuary retains all right, title and interest to and in, and ownership of, the Software and the Documentation, including, but not limited to, all copyrights, patents, trademarks, and other intellectual property rights relating thereto. Customer will have no rights with respect to the Software or the Documentation other than those expressly granted under this Agreement. Estuary shall own and retain all right, title and interest in and to any software, applications, inventions or other technology developed in connection with the use of the Software, and all intellectual property rights related thereto.

**1.6.** Documentation. Customer may copy and use (and permit the Authorized Users to copy and use) the Documentation solely in connection with the use of the Software under this Agreement.

**1.7.** Third-Party Products. In delivery of the Services, Estuary may employ certain Third-Party Products, as identified in the Documentation. The Third-Party Products are subject to specific license provisions and restrictions that are different from, or additional to, those contained herein. Customer acknowledges and agrees to be bound by and comply with such third-party license provisions and restrictions in the event such conditions do not conflict with this Agreement or the Order Form. The Parties agree that in the event of conflict, this Agreement and the Order Form shall prevail. Upon request, Estuary will provide Customer with information necessary to obtain copies of the Third-Party Product licenses.

## 2. Maintenance & Support Services. Estuary will provide support services (“Maintenance and Support Services”) to Customer for the Software during the Term of the Agreement as set forth in Exhibit 1.

## 3. Fees and Payment Terms. Customer will pay such Estuary fees set forth in any Order Form (the “Fees”), in accordance with the applicable payment schedules set forth therein. Except as provided in this Agreement, all payments of Fees are non-refundable. All Fees and other amounts stated in this Agreement, any Order Forms, or on any invoice shall be paid in the currency designated in such document. Customer will pay any applicable sales, use, or other taxes related to the services provided hereunder, exclusive of income taxes and payroll taxes relating to Estuary’s employees.

## 4. Term and Termination.

**4.1.** Term. The term of this Agreement (“Term”) begins on the Effective Date and will continue unless and until terminated in accordance with Section 4.2 below. The term of each Order Form will be set forth in such Order Form.

**4.2.** Termination. Either Party may terminate this Agreement: (i) upon thirty (30) days’ notice to the other Party if the other Party breaches a material term of this Agreement, and the breach remains uncured at the expiration of such thirty (30) day period; or (ii) immediately, in the case of a Trial Subscription or if the other Party becomes the subject of a petition in bankruptcy or any other proceeding relating to insolvency, liquidation, or assignment for the benefit of creditors. Estuary may also terminate this Agreement upon written notice to Customer under the limited circumstances set forth in Section 8.2 below. Termination of the Agreement will result in termination of any Order Form in effect.

**4.3.** Effect of Expiration or Termination. If an Order Form expires or is terminated, then this Agreement will remain in effect (unless and until terminated for breach in accordance with Section 4.2) with respect to any Order Forms that remain in effect (the “Surviving Agreements”). Upon any termination of this Agreement and/or any termination or expiration of an Order Form, the following provisions will apply (except with respect to any Surviving Agreements): (i) Customer will pay Estuary for any amounts payable hereunder as of the effective date of such termination or expiration; (ii) all rights and licenses granted hereunder to Customer (as well as any rights granted to any Authorized Users) will immediately cease, including, but not limited to, all use of the Software and the Documentation; and (iii) each Party will either return to the other Party or provide the other Party with written certification of the destruction of all documents, computer files and other materials containing any Confidential Information (as defined below) of such other Party that are in the first Party’s possession or control.

**4.4.** Survival. The following provisions will survive any termination or expiration of this Agreement: Section 1.4 (“Customer Data”), Section 1.5 (“Title”), Section 4.3 (“Effect of Expiration or Termination”), this Section 4.4 (“Survival”), Section 5 (“Confidentiality”), Section 6.3 (Disclaimer), Section 7 (“Liability”), Section 8 (“Intellectual Property Infringement”) and Section 9 (“Miscellaneous Provisions”).

**4.5.** Suspension. Without limiting Estuary’s other remedies (including any termination rights) set forth in this Agreement, Estuary reserves the right to suspend Customer’s access to or prohibit the use of the Services (a) if Fees are 30 days or more overdue, and are not otherwise subject to a good faith dispute; (b) if Estuary deems such suspension reasonably necessary as a result of Customer’s material breach of this Agreement; (c) if Estuary reasonably determines suspension is necessary to avoid material harm to Estuary or its customers; or (d) as required by applicable law or at the request of a governmental entity.

## 5. Confidentiality.

**5.1.** Definition of Confidential Information. For the purposes of this Agreement, “Confidential Information” means: (i) with respect to Estuary, the Software, any and all object code and source code relating thereto, the Documentation, all pricing and fees relating to the Services, as well as any non-public information or material regarding Estuary’s legal or business affairs, finances, technologies, customers, properties, or data, and (ii) with respect to Customer, the Customer Data and any other non-public information or material regarding Customer’s legal or business affairs, finances, technologies, customers, properties, or data. Notwithstanding any of the foregoing, Confidential Information does not include information which: (a) is or becomes public knowledge without any action by or on behalf of, or involvement of, the Party to which the Confidential Information is disclosed (the “Receiving Party”), (b) is documented as being known to the Receiving Party prior to its disclosure by the other Party (the “Disclosing Party”), (c) is independently developed by the Receiving Party without reference or access to the Confidential Information of the Disclosing Party and is so documented, or (d) is obtained by the Receiving Party without restrictions on use or disclosure from a third person who, to the Receiving Party’s knowledge, did not receive it, directly or indirectly, from the Disclosing Party.

**5.2.** Use and Disclosure of Confidential Information. The Receiving Party will, with respect to any Confidential Information disclosed by the Disclosing Party: (i) use such Confidential Information only in connection with the Receiving Party’s performance of this Agreement, (ii) subject to Section 5.4 below, restrict disclosure of such Confidential Information within the Receiving Party’s organization to only those of the Receiving Party’s employees who have a need to know such Confidential Information in connection with the Receiving Party’s performance of this Agreement, and (iii) not disclose such Confidential Information to any third party unless authorized in writing by the Disclosing Party to do so.

**5.3.** Protection of Confidential Information. The Receiving Party will protect the confidentiality of any Confidential Information disclosed by the Disclosing Party using at least the degree of care that it uses to protect its own confidential information (but no less than a reasonable degree of care).

**5.4.** Compliance by Personnel. The Receiving Party will, prior to providing an employee or representative access to any Confidential Information of the Disclosing Party, inform such employee or representative of the confidential nature of such Confidential Information and require such employee to comply with the Receiving Party’s obligations hereunder with respect to such Confidential Information.

**5.5.** Required Disclosures. In the event the Receiving Party becomes legally compelled (by any governmental or other regulatory authority or by a court or other authority of competent jurisdiction, including pursuant to oral questions, interrogatories, request for information or documents, subpoena, court order, civil investigative demand, or similar legal process) to disclose any of the Confidential Information of the Disclosing Party or take any other action with respect thereto prohibited by this Section, the Receiving Party will provide the Disclosing Party with prompt written notice of such order.

**5.6.** Remedy. Breach of the confidentiality obligations set forth in this Section may have no adequate remedy in damages and may cause irreparable damage to the Disclosing Party and, therefore, the Disclosing Party will have the right to seek injunctive relief, specific performance or other equitable relief (without the need to post a bond or other security), and to recover the amount of damages (including, without limitation, reasonable attorneys’ fees and expenses) incurred in connection with such breach or threatened breach, in addition to any other rights it may have in law or equity.

## 6. Representations and Warranties; Disclaimer.

**6.1.** Power and Authority. Each Party represents and warrants that it has the full right, power, and authority to enter into this Agreement and to discharge its obligations hereunder.

**6.2.** Additional Representations and Warranties of Estuary. Estuary further represents and warrants that: (i) any Open Source Software included in the Software does not and will not interact with Customer’s own software in such a way that would have the effect of requiring that Customer’s own software, or any portion thereof, to be: (a) disclosed or distributed in source code form, (b) licensed for the purpose of making derivative works, (c) redistributed, or (d) licensed under any open source or free software license or licensing scheme; (ii) to Estuary’s knowledge, after reasonable inquiry consistent with standard industry practices, the Software, as delivered to Customer, does not contain any Destructive Elements; (iii) the Software, as delivered to Customer, shall materially conform with the Documentation for a period of thirty (30) days from delivery to Customer; and (iv) Estuary shall perform any Maintenance & Support Services and Professional Services in a professional and workmanlike manner in accordance with prevailing industry standards and practices.

**6.3.** Disclaimer. Estuary cannot guarantee that every error that arises during delivery of the Services or problem raised by Customer will be resolved. THE SOFTWARE (INCLUDING ITS COMPONENTS AND ANY UPDATES), THE DOCUMENTATION, AND ANY OTHER MATERIALS PROVIDED HEREUNDER, AS WELL AS ANY OTHER SERVICES PROVIDED UNDER THIS AGREEMENT, ARE PROVIDED “AS IS,” AND EXCEPT AS SET FORTH IN SECTION 6.1 AND SECTION 6.2, NEITHER PARTY MAKES ANY REPRESENTATIONS OR WARRANTIES WITH RESPECT TO ANY OF THE FOREGOING OR OTHERWISE IN CONNECTION WITH THIS AGREEMENT, AND HEREBY DISCLAIMS ANY AND ALL IMPLIED AND STATUTORY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, ANY IMPLIED WARRANTIES OF TITLE, MERCHANTABILITY, NON-INFRINGEMENT, FITNESS FOR A PARTICULAR PURPOSE, ERROR-FREE OR UNINTERRUPTED OPERATION, AND ANY WARRANTIES ARISING FROM A COURSE OF DEALING, COURSE OF PERFORMANCE, OR USAGE OF TRADE. To the extent that a Party may not as a matter of applicable law disclaim any implied warranty, the scope and duration of such warranty will be the minimum permitted under such law.

## 7. Liability.

**7.1.** Liability Exclusion. NEITHER PARTY WILL BE LIABLE TO THE OTHER PARTY (NOR TO ANY PERSON CLAIMING RIGHTS DERIVED FROM THE OTHER PARTY'S RIGHTS) FOR CONSEQUENTIAL, INCIDENTAL, INDIRECT, PUNITIVE, SPECIAL, OR EXEMPLARY DAMAGES OF ANY KIND, OR FOR ANY LOST REVENUES OR PROFITS, LOSS OF USE, LOSS OF COST OR OTHER SAVINGS, LOSS OF DATA, OR LOSS OF GOODWILL OR REPUTATION, (IN EACH CASE WHETHER DIRECT OR INDIRECT) WITH RESPECT TO ANY CLAIMS BASED ON CONTRACT, TORT, OR OTHERWISE (INCLUDING NEGLIGENCE AND STRICT LIABILITY) ARISING OUT OF OR RELATING TO THE SERVICES, THE SOFTWARE, THE DOCUMENTATION, OR THE MAINTENANCE & SUPPORT SERVICES, OR OTHERWISE ARISING OUT OF OR RELATING TO THIS AGREEMENT, ANY STATEMENT OF WORK, OR ANY ORDER FORM, REGARDLESS OF WHETHER SUCH PARTY WAS ADVISED, HAD OTHER REASON TO KNOW, OR IN FACT KNEW OF THE POSSIBILITY THEREOF.

**7.2.** Limitation of Damages. EACH PARTY’S MAXIMUM, CUMULATIVE LIABILITY ARISING OUT OF OR RELATING TO THE SERVICES, THE SOFTWARE, THE DOCUMENTATION, THE MAINTENANCE & SUPPORT SERVICES, ANY PROFESSIONAL SERVICES, OR OTHERWISE ARISING OUT OF OR RELATING TO THIS AGREEMENT, ANY STATEMENT OF WORK, OR ANY ORDER FORM, REGARDLESS OF THE CAUSE OF ACTION (WHETHER IN CONTRACT, TORT, INDEMNITY, BREACH OF WARRANTY OR OTHERWISE), WILL NOT EXCEED, FOR ALL CLAIMS IN THE AGGREGATE, THE AMOUNTS PAID BY CUSTOMER TO ESTUARY UNDER THIS AGREEMENT DURING THE TWELVE (12) MONTHS PRECEDING THE DATE ON WHICH THE FIRST OF ANY CLAIMS BY SUCH PARTY FIRST ARISES.

**7.3.** Exceptions. Notwithstanding anything to the contrary, the exclusions and limitations set forth in Section 7.1 and Section 7.2 will not apply with respect to (i) any damages arising from a Party’s fraud, gross negligence, or willful misconduct, (ii) infringement or misappropriation of a Party’s intellectual property rights, (iii) Customer’s breach of Section 1.3, or (iv) Customer’s breach of its obligation to pay Fees to Estuary under this Agreement.

## 8. Indemnification and Intellectual Property Infringement.

**8.1.** Indemnification. Each Party (the “Indemnifying Party”) will defend, hold harmless, and indemnify the other Party (the “Indemnified Party”) and its officers, directors, and employees from and against any and all claims, actions, and lawsuits brought by a third party that are proven in a recognized court of law (“Third-Party Claims”), and will pay any settlements, awards and reasonable attorney’s fees associated with such Third-Party Claims (“Losses”), to the extent the Third-Party Claim is based on (i) an assertion that the Indemnifying Party’s software, data or other documentation or materials infringe upon any copyright or trade secret of a third party or (ii) a violation of Section 1.3; provided, however, that notwithstanding the foregoing, Estuary will have no obligation with respect to any Third-Party Claim to the extent the Third-Party Claim arises from or relates to: (i) use of the Services or Software in a manner that is not in accordance with this Agreement or the Documentation, (ii) any modification made to the Software by Customer or any third party, or (iii) use of the Services or Software in combination with any other software, system, device, or process. The foregoing obligations will be subject to the Indemnified Party: (a) promptly notifying the Indemnifying Party of the Third-Party Claim, (b) providing Indemnifying Party with reasonable cooperation in the defense of the Third-Party Claim, and (c) providing Indemnifying Party with sole control over the defense and negotiations for a settlement or compromise.

**8.2.** Infringement Claims. In the event that (i) any Software is held to infringe or misappropriate the rights of a third party and/or the use of any Software is enjoined, or (ii) Estuary believes that there is a risk that any Software could be found to infringe or misappropriate the rights of a third party, Estuary will, if possible on commercially reasonable terms, at its own expense and option: (a) procure for Customer the right to continue to use such Software, (b) replace the components of such Software that are at issue with other components with the same or similar functionality, or (c) suitably modify such Software so that it is non-infringing and includes the same or similar functionality. If none of the foregoing options are available to Estuary on commercially reasonable terms, Estuary may terminate the Agreement or the Order Form to which such Software relates without further liability to Customer, and in the event of such termination, Estuary will refund to Customer an amount equal to the license fee paid by Customer for the infringing version(s) for the then-current period, less a deduction reasonably determined by Estuary to account for Customer’s use of such Software. This Section 8.2, together with the indemnity provided under Section 8.1, states Customer’s sole and exclusive remedy, and Estuary’s sole and exclusive liability, regarding infringement or misappropriation of any intellectual property rights of a third party.

## 9. Miscellaneous Provisions.

**9.1.** Notices. All notices under this Agreement must be in writing and will be deemed to have been duly given when received, if personally delivered; when receipt is electronically confirmed, if transmitted by facsimile or e-mail; the day after it is sent, if sent for next day delivery by recognized overnight delivery service; and upon receipt, if sent by certified or registered mail, return receipt requested to each party at its address or e-mail address provided below or with the most recently executed Order Form.

If to Estuary:

Estuary Technologies, Inc.

244 5th Avenue, Suite 1277

New York, NY 10001

**9.2.** Relationship of the Parties. Each Party is an independent contractor of the other Party. Nothing herein will constitute a partnership between or joint venture by the Parties, or constitute either Party the agent of the other. Each Party and its managers, members, directors, officers, employees, and agents may not represent that they are employees or agents of the other Party, nor may they in any manner hold themselves out to be employees or agents of the other Party.

**9.3.** Assignment; Change of Control. Neither Party may assign or otherwise transfer this Agreement without the prior, written consent of the other Party, which consent will not unreasonably be withheld; provided, however, that notwithstanding the foregoing, either Party may, without any obligation to obtain the other Party’s consent, assign or transfer this Agreement: (i) to any of its Affiliates, or (ii) in connection with a change of control transaction (whether by merger, consolidation, sale of equity interests, sale of all or substantially all assets, or otherwise) with respect to such Party or its business to which this Agreement relates. Any assignment or other transfer in violation of this Section will be null and void. Subject to the foregoing, this Agreement will be binding upon and inure to the benefit of the Parties hereto and their permitted successors and assigns

**9.4.** Disclosure and Publicity. Unless otherwise provided herein or in an Order Form, Estuary shall not use Customer’s name and logos on its website, or in any other public marketing materials, without Customer’s express written consent; provided, however, that Customer hereby expressly consents to Estuary’s use of Customer’s name and logos solely in order to refer to Customer as a customer of Estuary in information provided to investors and prospective investors. Estuary’s use of Customer’s name and logos shall comply with any branding guidelines and other instructions provided to Estuary by Customer in writing. Neither Party shall directly or indirectly obtain or attempt to obtain during the Term hereof or at any time thereafter, any right, title or interest in or to the other Party’s names, trade names, trademarks, service marks, or logos.

**9.5.** Force Majeure. Except with respect to failure to pay any amount due under this Agreement, non-performance of either Party will be excused to the extent that performance is rendered impossible by strike, fire, flood, governmental acts (including those related to a public health crisis or pandemic), orders or restrictions, failure of suppliers, or any other reason where failure to perform is beyond the control and not caused by the negligence of the non-performing Party (each, a “Force Majeure Event”).

**9.6.** Choice of Law. This Agreement, and any disputes directly or indirectly arising from or relating to this Agreement, will be governed by and construed in accordance with the laws of the State of New York, without regard to principles of conflicts of law.

**9.7.** Waiver of Jury Trial. EACH PARTY IRREVOCABLY WAIVES TRIAL BY JURY IN ANY ACTION OR PROCEEDING WITH RESPECT TO THIS AGREEMENT.

**9.8.** Modification. No modification of, or amendment to, this Agreement will be effective unless in writing signed by authorized representatives of both Parties.

**9.9.** No Waiver. The rights and remedies of the Parties to this Agreement are cumulative and not alternative. No waiver of any rights is to be charged against any Party unless such waiver is in writing signed by an authorized representative of the Party so charged. Neither the failure nor any delay by any Party in exercising any right, power, or privilege under this Agreement will operate as a waiver of such right, power, or privilege, and no single or partial exercise of any such right, power, or privilege will preclude any other or further exercise of such right, power, or privilege or the exercise of any other right, power, or privilege.

**9.10.** Severability. If any provision of this Agreement is held invalid or unenforceable by any court of competent jurisdiction, the other provisions of this Agreement will remain in full force and effect, and, if legally permitted, such offending provision will be replaced with an enforceable provision that as nearly as possible effects the Parties’ intent.

**9.11.** Entire Agreement. This Agreement (including the Appendices attached hereto, any Order Forms) contains the entire understanding of the Parties with respect to the subject matter hereof and supersedes all prior agreements and commitments with respect thereto. There are no other oral or written understandings, terms or conditions and neither Party has relied upon any representation, express or implied, not contained in this Agreement.

**9.12.** Execution in Counterparts. This Agreement may be executed in counterparts (which may be exchanged by facsimile or .pdf copies), each of which will be deemed an original, but all of which together will constitute the same Agreement.

## 10. Definitions. The definitions for some of the defined terms used in this Agreement are set forth below. The definitions for other defined terms are set forth elsewhere in this Agreement.

**10.1.** “Affiliate” means, with respect to any entity, any other entity that, directly or indirectly, through one or more intermediaries, controls, is controlled by, or is under common control with, such entity. The term “control” means the possession, directly or indirectly, of the power to direct or cause the direction of the management and policies of an entity, whether through the ownership of voting securities, by contract, or otherwise.

**10.2.** “Authorized User” means an employee of Customer who has been authorized by Customer to use the Software.

**10.3.** “Destructive Elements” means computer code, programs, or programming devices that are intentionally designed to disrupt, modify, access, delete, damage, deactivate, disable, harm, or otherwise impede in any manner, including aesthetic disruptions or distortions, the operation of any software, firmware, hardware, computer system, or network (including, without limitation, “Trojan horses,” “viruses,” “worms,” “time bombs,” “time locks,” “devices,” “traps,” “access codes,” or “drop dead” or “trap door” devices) or any other harmful, malicious, or hidden procedures, routines or mechanisms that would cause the same to cease functioning or to damage or corrupt data, storage media, programs, equipment, or communications, or otherwise interfere with operations.

**10.4.** “Documentation” means any user guides and other documentation for the Software that Estuary provides to Customer.

**10.5.** “Open Source Software” means individual software components that are provided with the Software, for which the source code is made generally available, and that are licensed under the terms of various published open source software license agreements or copyright notices accompanying such software components.

**10.6.** “Software” means: (i) any software that is described in an Order Form, and (ii) any Updates to that software that are made available to Customer from time to time.

**10.7.** “Order Form” means an order or online registration that is signed or accepted by authorized representatives of both Parties and that sets forth: (i) the Services and Software being ordered, (ii) the applicable Term (as defined above) and any applicable usage limitations, (iii) the applicable fees, and (iv) other mutually-agreed upon terms and conditions relating to such order.

**10.8.** “Third-Party Products” means certain software or materials owned by third parties and sublicensed to Customer by Estuary as part of the Software, including, but not limited to, Open Source Software.

**10.9.** “Update” means any and all new releases, new versions, patches, updates, and upgrades for the Software that Estuary makes generally available to Customer.

# EXHIBIT 1

## SERVICE LEVEL AGREEMENT (“SLA”)

## 1. Definitions. The following terms, as used in this SLA, shall have the meanings set forth below. Terms used in this SLA but not defined below shall have the meanings ascribed to them elsewhere in this SLA or in the Agreement.

**1.1.** “Availability” means the time when Estuary support personnel are accessible.

**1.2.** “Designated Contacts” means those Customer employees designated by Customer as authorized contacts to report, discuss, and resolve issues contemplated by this SLA. Customer may modify its list of Designated Contacts upon written notice to Estuary.

**1.3.** “Error” means any reproducible failure of the Software to perform materially in accordance with the Documentation or, for non-reproducible failures, an event wherein Customer provides operational information (error message, debug log output, etc.) to Estuary that definitively determines that the Error was due solely to the Software.

**1.4.** “Error Correction” means either a modification to the Software that substantially conforms such Software to the Documentation or a Workaround.

**1.5.** “Regular Business Hours (RBH)” means 9:00 AM Eastern Time to 7:00 PM Eastern Time, Monday through Friday, excluding local, state, and federal holidays observed by Estuary.

**1.6.** “Resolution” means that an Error Correction or an answer to an inquiry has been delivered to Customer.

**1.7.** “Response Time” means the time required for a Estuary support engineer to respond to Customer confirming receipt of Error notification and informing Customer if additional information is needed to proceed with analysis.

**1.8.** “Severity 1 Error” means either (i) an Error in the Software that renders Customer unable to conduct its business, in whole or in part, with respect to any one or more systems utilizing the Software; or (ii) an Error in the Software that causes a critical process to be inaccessible, to fail, or to materially underperform with respect to any one or more systems utilizing the Software that has a material adverse effect on Customer.

**1.9.** “Severity 2 Error” means an Error in the Software that has a significant adverse impact on Customer’s operation that does not rise to the level of a Severity 1 Error.

**1.10.** “Severity 3 Error” means that the Software is operational with Errors that do not fall within the definition of Severity 1 or Severity 2 Errors.

**1.11.** “Support Request” means the logging of a service-impacting condition by the Estuary operations center on behalf of Customer.

**1.12.** “Supported Environment” means the prescribed hardware and operating system configurations for the Software as set forth in the Documentation.

**1.13.** “Workaround” shall mean a change in a procedure or routine that, when observed in the regular operation of the Software, eliminates any material adverse effect on Customer of the Error without imposing additional expense or an unreasonable burden upon Customer.

## 2. Notification.

**2.1.** Support Hours and Contact Methods.

**Support Contact Methods**

| Method | Contact |
| --- | --- |
| E-mail Support | support@estuary.dev |

**2.2.** Error Reporting. If Customer believes that an Error has occurred, Customer must initiate a Support Request by contacting Estuary support in accordance with the method of contact set forth in the table above. For all Support Requests, Customer’s Designated Contacts must include for each Error: (i) a general description of the Error and the recommended severity level, which shall be suggested by Customer reasonably and in good faith; (ii) a reproducible test case or operational information (error message, debug log output, etc.); and (iii) upon request from Estuary, remote access to the network on which the Error has occurred. Customer shall supply Estuary with any and all information as is requested by Estuary and as is reasonably available to Customer that is necessary to respond to the inquiry. In the event Customer does not promptly supply the information described above, Estuary shall not be obligated to respond or resolve within the required timetables set forth in Section 5 below, unless and until Customer supplies such information, at which time the below timetables shall re-commence. If there is a disagreement as to the severity level of a particular Error, the issue shall be escalated to a designated technical lead for Estuary and the designated Customer technical representative who shall discuss the business impact on Customer. The Parties shall undertake reasonable efforts to agree on the severity level of Errors, however, absent an agreement between the Parties, the final determination of the severity level of Errors shall be made by Estuary at its sole, but reasonable, discretion.

**2.3.** Estuary Response. Estuary will make reasonable commercial efforts to respond to Support Requests within the timetables set forth in Section 4, and to provide Workarounds and/or Resolutions in the time frame indicated. However, situations may arise where devising a Workaround or Resolution to the reported inquiry may take longer due to the difficulty of the problem.

**2.4.** Designated Contacts. Customer will be allowed up to five (5) Designated Contacts, and one of the Designated Contacts must be identified as the administrator for Customer. Only Customer employees who are Designated Contacts may contact Estuary support to initiate Support inquiries.

## 3. Customer Responsibilities. Customer shall be and remain responsible for the following:

**3.1.** Providing all necessary support for Support Requests from Authorized Users;

**3.2.** Complying with the specifications of the Supported Environment for the Software;

**3.3.** Allowing Estuary access to the Customer environment for support purposes. Access shall be remote or on-site, as necessary and as requested by Estuary. Access will be permitted under direct control of Customer; and

**3.4.** Providing Estuary with such information, specifications, or other information as may reasonably be required by Estuary to properly respond to the inquiry in a timely fashion.

## 4. Time to Respond (“TTR”). Estuary shall make reasonable efforts to meet the following TTR levels for Support Requests properly submitted by Customer as set forth in the table below:

| Severity Level | Time to Respond |
| --- | --- |
| Severity 1 Error | Availability: 24x7x365 Response Time: 4 hours Resolution Time: Work continuously to Resolution, with a goal of 1 business day |
| Severity 2 Error | Availability: 24x7x365 Response Time: 12 hours Resolution Time: Work continuously to Resolution, with a goal of 3 business days |
| Severity 3 Error | Availability: Regular Business Hours Response Time: 48 hours during Regular Business Hours Resolution Time: 15 business days, unless otherwise agreed by the Parties |

## 5. Exclusions.

**5.1.** The response times set forth in Section 4 apply only to those Maintenance & Support Services within the scope of this SLA, and do not apply to Customer-requested service interruptions or to any use of the Software by Customer not consistent with the Agreement.

**5.2.** Under no circumstances will Estuary be liable or responsible for Errors or other issues with the Maintenance & Support Services that involve:

**5.2.1.** Support Requests erroneously opened by Customer;

**5.2.2.** Accounts provided to Customer for testing or development purposes;

**5.2.3.** Support Requests opened by Customer for service monitoring purposes only;

**5.2.4.** Support Requests related to Customer maintenance, configurations, negligence, accidents, or omissions;

**5.2.5.** The Software being serviced or modified by anyone other than Estuary or by a third party authorized by Estuary;

**5.2.6.** Force Majeure Events; or

**5.2.7.** Matters that arise from failures of Customer’s systems or servers.
$legal_terms$
);

commit;
