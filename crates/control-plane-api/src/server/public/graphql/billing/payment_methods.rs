use async_graphql::SimpleObject;

#[derive(Debug, Clone, SimpleObject)]
pub struct PaymentMethod {
    pub id: String,
    #[graphql(name = "type")]
    pub type_: String,
    pub billing_details: PaymentMethodBillingDetails,
    pub card: Option<CardPaymentMethodDetails>,
    pub us_bank_account: Option<UsBankAccountPaymentMethodDetails>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct PaymentMethodBillingDetails {
    pub name: Option<String>,
}

impl From<&stripe::BillingDetails> for PaymentMethodBillingDetails {
    fn from(details: &stripe::BillingDetails) -> Self {
        Self {
            name: details.name.clone(),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct CardPaymentMethodDetails {
    pub brand: String,
    pub last4: String,
    pub exp_month: i64,
    pub exp_year: i64,
}

impl From<&stripe::CardDetails> for CardPaymentMethodDetails {
    fn from(card: &stripe::CardDetails) -> Self {
        Self {
            brand: card.brand.clone(),
            last4: card.last4.clone(),
            exp_month: card.exp_month,
            exp_year: card.exp_year,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UsBankAccountPaymentMethodDetails {
    pub bank_name: Option<String>,
    pub last4: Option<String>,
    pub account_holder_type: Option<String>,
}

impl From<&stripe::PaymentMethodUsBankAccount> for UsBankAccountPaymentMethodDetails {
    fn from(account: &stripe::PaymentMethodUsBankAccount) -> Self {
        Self {
            bank_name: account.bank_name.clone(),
            last4: account.last4.clone(),
            account_holder_type: account
                .account_holder_type
                .map(|kind| kind.as_str().to_string()),
        }
    }
}

impl From<&stripe::PaymentMethod> for PaymentMethod {
    fn from(pm: &stripe::PaymentMethod) -> Self {
        Self {
            id: pm.id.to_string(),
            type_: pm.type_.as_str().to_string(),
            billing_details: PaymentMethodBillingDetails::from(&pm.billing_details),
            card: pm.card.as_ref().map(CardPaymentMethodDetails::from),
            us_bank_account: pm
                .us_bank_account
                .as_ref()
                .map(UsBankAccountPaymentMethodDetails::from),
        }
    }
}
