import { initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyDQ9jKtgm-p9JJkKla07GdoEK64dDSgrp4",
  authDomain: "driftforce-861ce.firebaseapp.com",
  projectId: "driftforce-861ce",
  storageBucket: "driftforce-861ce.firebasestorage.app",
  messagingSenderId: "439455252696",
  appId: "1:439455252696:web:b8fd6f3339b68e28d157ad",
  measurementId: "G-W8RSBZV3D5"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);