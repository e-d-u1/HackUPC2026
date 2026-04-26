const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
const recognition = new SpeechRecognition();

recognition.lang = 'es-ES'; // cambia según tu app
recognition.interimResults = false;
recognition.maxAlternatives = 1;

recognition.onresult = async (event) => {
  const transcript = event.results[0][0].transcript;
  console.log('Transcripción:', transcript);
  
  // Aquí envías el texto a tu backend en lugar del audio
  const response = await fetch('/procesar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ texto: transcript })
  });
};

recognition.onerror = (event) => {
  console.error('Error:', event.error);
};

// Iniciar
recognition.start();