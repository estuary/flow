import React, { useEffect } from 'react';
import { loadReoScript } from 'reodotdev'

const MESSAGE_TYPE = 'estuary.docs.hideNavBar';
const BODY_CLASS = 'running-in-iframe';
const REO_DOT_DEV_ID = "a2955ffcdd9029c";

// Default implementation, that you can customize
export default function Root({ children }) {
    useEffect(() => {
        // Load in reo.dev scripts
        const reoPromise = loadReoScript({
            clientID: REO_DOT_DEV_ID
        });
        reoPromise.then(Reo => {
            Reo.init({
                clientID: REO_DOT_DEV_ID
            });
        })

        // Listen for messaging from dashboard for styling light/dark mode
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
