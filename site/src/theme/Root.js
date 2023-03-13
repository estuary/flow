import React, { useEffect } from 'react';

const MESSAGE_TYPE = 'estuary.docs.hideNavBar';

const BODY_CLASS = 'running-in-iframe';

// Default implementation, that you can customize
export default function Root({ children }) {
    useEffect(() => {
        const handleMessageListener = (event) => {
            if (event.origin === 'http://localhost:3000' || event.origin === 'https://dashboard.estuary.dev') {
                if (event.data?.type === MESSAGE_TYPE) {
                    window.document.body.classList.add(BODY_CLASS);
                }
            }
        };

        window.addEventListener('message', handleMessageListener);
        return () => {
            window.removeEventListener('message', handleMessageListener);
        };
    }, []);

    return <>{children}</>;
}
